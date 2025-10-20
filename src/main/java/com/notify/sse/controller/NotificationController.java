package com.notify.sse.controller;

import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
@RequestMapping("/notifications")
@Slf4j
public class NotificationController {

    // Thread-safe list to store emitters
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    @GetMapping("/subscribe")
    public SseEmitter subscribe() {
    	log.info("client is Subscribing and opening the connection.");
        SseEmitter emitter = new SseEmitter(600000L); // no timeout
        emitters.add(emitter);
        log.info("New subscriber added, total subscribers: {}", emitters.size());

        // Remove emitter on completion/error/timeout
        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.info("Emitter completed, remaining: {}", emitters.size());
        });
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            log.info("Emitter timed out, remaining: {}", emitters.size());
        });
        emitter.onError(e -> {
            emitters.remove(emitter);
            log.warn("Emitter error: {}, remaining: {}", e.getMessage(), emitters.size());
        });
        try {
        	emitter.send(SseEmitter.event().data("connected"));
			emitter.send(SseEmitter.event().comment("connected"));
		} catch (IOException e) {
			emitter.completeWithError(e);
            log.info("Removed emitter, total emitters: {}", emitters.size());
		}
        return emitter;
    }

    @PostMapping("/send")
    public void sendNotification(@RequestParam("user") String user, @RequestBody String message) {
    	log.info("Sending notification to {} subscribers", emitters.size());
    	String msg = user + ": " + message;
    	for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("notification").data(msg, MediaType.TEXT_PLAIN));
            } catch (IOException e) {
            	emitter.completeWithError(e);
                log.info("Removed emitter, total emitters: {}", emitters.size());
            }
        }
    }
    
    @PreDestroy
    public void cleanUp() {
    	log.info("Final cleanup before bean destruction...");
        emitters.clear();
    }

    @EventListener(ContextClosedEvent.class)
    public void onShutdown() {
        log.info("ContextClosedEvent received — shutting down, Active SSE emitters: {}", emitters.size());
        synchronized (emitters) {
            for (SseEmitter emitter : emitters) {
                try {
                    emitter.complete();
                } catch (Exception e) {
                    log.warn("Error completing emitter: {}", e.getMessage());
                }
            }
            emitters.clear();
        }
        log.info("All SSE emitters closed cleanly.");
    }
    
}
