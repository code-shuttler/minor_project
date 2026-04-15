package com.backend.models.timetable;

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

import com.backend.models.autogeneration.TabuGenerationService;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/timetables")
@Validated
public class TimetableController {

    @Autowired
    private TimetableService timetableService;

    @Autowired
    private TabuGenerationService tabuGenerationService;

    @GetMapping
    public ResponseEntity<List<TimetableResponseModel>> getTimetables() {
        return ResponseEntity.ok(timetableService.getAllTimetables());
    }

    @GetMapping("/{id}")
    public ResponseEntity<TimetableResponseModel> getTimetableById(@PathVariable Long id) {
        return ResponseEntity.ok(timetableService.getTimetableById(id));
    }

    @PostMapping
    public ResponseEntity<List<TimetableResponseModel>> autoGenerateTimetable(@Valid @RequestBody TimetableAutoGenerateRequestModel request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(tabuGenerationService.generateTimetable(request));
    }

    @PutMapping("/{id}")
    public ResponseEntity<TimetableResponseModel> updateTimetable(@PathVariable Long id,
            @Valid @RequestBody TimetableRequestModel request) {
        return ResponseEntity.ok(timetableService.updateTimetable(id, request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTimetable(@PathVariable Long id) {
        timetableService.deleteTimetable(id);
        return ResponseEntity.noContent().build();
    }
}