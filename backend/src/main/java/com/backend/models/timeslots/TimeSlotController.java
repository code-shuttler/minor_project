package com.backend.models.timeslots;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/time-slots")
@Validated
public class TimeSlotController {

    @Autowired
    private TimeSlotService timeSlotService;

    @GetMapping
    public ResponseEntity<List<TimeSlotResponseModel>> getTimeSlots() {
        return ResponseEntity.ok(timeSlotService.getAllTimeSlots());
    }

    @GetMapping("/{id}")
    public ResponseEntity<TimeSlotResponseModel> getTimeSlotById(@PathVariable Long id) {
        return ResponseEntity.ok(timeSlotService.getTimeSlotById(id));
    }

    @PostMapping
    public ResponseEntity<TimeSlotResponseModel> createTimeSlot(@Valid @RequestBody TimeSlotRequestModel request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(timeSlotService.createTimeSlot(request));
    }

    @PutMapping("/{id}")
    public ResponseEntity<TimeSlotResponseModel> updateTimeSlot(@PathVariable Long id,
            @Valid @RequestBody TimeSlotRequestModel request) {
        return ResponseEntity.ok(timeSlotService.updateTimeSlot(id, request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTimeSlot(@PathVariable Long id) {
        timeSlotService.deleteTimeSlot(id);
        return ResponseEntity.noContent().build();
    }
}
