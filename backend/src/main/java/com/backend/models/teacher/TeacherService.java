package com.backend.models.teacher;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.backend.exceptions.DuplicateResourceException;
import com.backend.exceptions.ResourceNotFoundException;
import com.backend.models.department.DepartmentEntity;
import com.backend.models.department.DepartmentRepository;
import com.backend.models.subject.SubjectEntity;
import com.backend.models.subject.SubjectRepository;

@Service
public class TeacherService {

    @Autowired
    private TeacherRepository teacherRepository;

    @Autowired
    private DepartmentRepository departmentRepository;

    @Autowired
    private SubjectRepository subjectRepository;

    public List<TeacherResponseModel> getAllTeachers() {
        return teacherRepository.findAll().stream().map(this::toResponse).toList();
    }

    public TeacherResponseModel getTeacherById(Long id) {
        TeacherEntity entity = teacherRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Teacher not found with id: " + id));
        return toResponse(entity);
    }

    public TeacherResponseModel createTeacher(TeacherRequestModel request) {
        DepartmentEntity department = getDepartmentOrThrow(request.getDepartmentId());
        Set<SubjectEntity> specializations = getSubjectsByIdsOrThrow(request.getSpecializationIds(), "specializationIds");

        TeacherEntity entity = new TeacherEntity();
        entity.setName(normalize(request.getName()));
        entity.setAbbreviation(normalize(request.getAbbreviation()));
        entity.setSpecializations(specializations);
        entity.setDepartment(department);

        try {
            TeacherEntity saved = teacherRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Teacher data violates a uniqueness or integrity rule");
        }
    }

    public TeacherResponseModel updateTeacher(Long id, TeacherRequestModel request) {
        TeacherEntity existing = teacherRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Teacher not found with id: " + id));

        DepartmentEntity department = getDepartmentOrThrow(request.getDepartmentId());
        Set<SubjectEntity> specializations = getSubjectsByIdsOrThrow(request.getSpecializationIds(), "specializationIds");

        existing.setName(normalize(request.getName()));
        existing.setAbbreviation(normalize(request.getAbbreviation()));
        existing.setSpecializations(specializations);
        existing.setDepartment(department);

        try {
            TeacherEntity saved = teacherRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Teacher data violates a uniqueness or integrity rule");
        }
    }

    public void deleteTeacher(Long id) {
        if (!teacherRepository.existsById(id)) {
            throw new ResourceNotFoundException("Teacher not found with id: " + id);
        }
        teacherRepository.deleteById(id);
    }

    private DepartmentEntity getDepartmentOrThrow(Long departmentId) {
        return departmentRepository.findById(departmentId)
                .orElseThrow(() -> new ResourceNotFoundException("Department not found with id: " + departmentId));
    }

    private Set<SubjectEntity> getSubjectsByIdsOrThrow(Set<Long> subjectIds, String fieldName) {
        Set<Long> requested = subjectIds == null ? Collections.emptySet() : new HashSet<>(subjectIds);
        if (requested.isEmpty()) {
            return new HashSet<>();
        }

        List<SubjectEntity> subjects = subjectRepository.findAllById(requested);
        if (subjects.size() != requested.size()) {
            Set<Long> foundIds = subjects.stream().map(SubjectEntity::getId).collect(Collectors.toSet());
            Set<Long> missingIds = requested.stream().filter(id -> !foundIds.contains(id)).collect(Collectors.toSet());
            throw new ResourceNotFoundException("Invalid " + fieldName + ", subject ids not found: " + missingIds);
        }
        return new HashSet<>(subjects);
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private TeacherResponseModel toResponse(TeacherEntity entity) {
        return new TeacherResponseModel(
                entity.getId(),
                entity.getName(),
                entity.getAbbreviation(),
                entity.getSpecializations().stream().map(SubjectEntity::getId).collect(Collectors.toSet()),
                entity.getDepartment().getId());
    }
}
