package com.notify.sse.service;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SseService {
	
	/*
	 * Thread-safe list to store emitters
	 * 
	 * stores just the emitters:
	 * private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();
	 * 
	 * stores multiple emitters of a user, will be useful if the user has multiple device/tabs/window: 
	 * private final ConcurrentHashMap<String, CopyOnWriteArrayList<SseEmitter>> emitters = new ConcurrentHashMap<>();
	 * 
	 * store a single emitter for a user:
	 */
	private final ConcurrentHashMap<String, SseEmitter> emitters = new ConcurrentHashMap<>();
	
	public SseEmitter addEmitter(String userId) {
		log.info("client is Subscribing and opening the connection.");
        SseEmitter emitter = new SseEmitter(600000L); // timeout 10mins
        SseEmitter oldEmitter = emitters.put(userId, emitter);
        if (oldEmitter != null) {
            oldEmitter.complete();
        }
        log.info("New subscriber added, total subscribers: {}", emitters.size());

        // Remove emitter on completion/error/timeout
        emitter.onCompletion(() -> {
            emitters.remove(userId, emitter);
            log.info("Emitter completed, remaining: {}", emitters.size());
        });
        emitter.onTimeout(() -> {
            emitters.remove(userId, emitter);
            log.info("Emitter timed out, remaining: {}", emitters.size());
        });
        emitter.onError(e -> {
            emitters.remove(userId, emitter);
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

	public void publishEvent(String userId, String message) {
		log.info("Sending notification to {} subscribers", emitters.size());
    	emitters.forEach((id, emitter) -> {
    		if (id.equals(userId)) return;// continue
			try {
	    		emitter.send(SseEmitter.event().name("notification").data(message, MediaType.TEXT_PLAIN));
	        } catch (IOException ex) {
	        	emitters.remove(id, emitter);
	        	emitter.completeWithError(ex);
	            log.info("Removed emitter, total emitters: {}", emitters.size());
	        }
		});
	}
	
	@PreDestroy
    public void cleanUp() {
    	log.info("Final cleanup before bean destruction...");
        emitters.clear();
    }

    @EventListener(ContextClosedEvent.class)
    public void onShutdown() {
        log.info("ContextClosedEvent received — shutting down, Active SSE emitters: {}", emitters.size());
        emitters.values().forEach(emitter -> {
            try {
                emitter.complete();
            } catch (Exception ex) {
                log.warn("Failed to complete emitter", ex);
            }
        });
        emitters.clear();
        log.info("All SSE emitters closed cleanly.");
    }

}
