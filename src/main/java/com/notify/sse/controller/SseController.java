package com.notify.sse.controller;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.notify.sse.service.SseService;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Sudharshan
 */

@RestController
@Slf4j
@RequestMapping("/sse")
public class SseController {
	
	@Autowired
	private SseService sseService;

	@GetMapping
    public SseEmitter subscribe(@RequestParam String userId) {
    	SseEmitter emitter = sseService.addEmitter(userId);
        return emitter;
    }

	@GetMapping("/notifications")
	public SseEmitter streamNotifications() {
	    SseEmitter emitter = new SseEmitter();
	    log.info("Starting notification processing...");
	    Executors.newSingleThreadExecutor().submit(() -> {
	        try {
	            for (int i = 1; i <= 5; i++) {
	                emitter.send("Update " + i);
	                log.info("processing job");
	                Thread.sleep(2000); 
	            }
	            emitter.send("Processing complete!");
	            log.info("Terminating the Connection!");
	            emitter.complete();
	        } catch (Exception e) {
	        	log.info("Error: {}", e.getMessage(), e);
	            emitter.completeWithError(e);
	        }
	    });
	
	    return emitter;
	}
	
}