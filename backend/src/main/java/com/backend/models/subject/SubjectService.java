package com.backend.models.subject;

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

@Service
public class SubjectService {

    @Autowired
    private SubjectRepository subjectRepository;

    @Autowired
    private DepartmentRepository departmentRepository;

    public List<SubjectResponseModel> getAllSubjects() {
        return subjectRepository.findAll().stream().map(this::toResponse).toList();
    }

    public SubjectResponseModel getSubjectById(Long id) {
        SubjectEntity entity = subjectRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Subject not found with id: " + id));
        return toResponse(entity);
    }

    public SubjectResponseModel createSubject(SubjectRequestModel request) {
        String normalizedName = normalize(request.getName());
        validateUniqueSubject(normalizedName, null);
        Set<DepartmentEntity> departments = getDepartmentsByIdsOrThrow(request.getDepartmentIds());

        SubjectEntity entity = new SubjectEntity();
        entity.setName(normalizedName);
        entity.setDepartments(departments);
        entity.setLecture(request.getLecture());
        entity.setTutorial(request.getTutorial());
        entity.setPractical(request.getPractical());

        try {
            SubjectEntity saved = subjectRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Subject with same name already exists");
        }
    }

    public SubjectResponseModel updateSubject(Long id, SubjectRequestModel request) {
        SubjectEntity existing = subjectRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Subject not found with id: " + id));

        String normalizedName = normalize(request.getName());
        validateUniqueSubject(normalizedName, id);
        Set<DepartmentEntity> departments = getDepartmentsByIdsOrThrow(request.getDepartmentIds());

        existing.setName(normalizedName);
        existing.setDepartments(departments);
        existing.setLecture(request.getLecture());
        existing.setTutorial(request.getTutorial());
        existing.setPractical(request.getPractical());

        try {
            SubjectEntity saved = subjectRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Subject with same name already exists");
        }
    }

    public void deleteSubject(Long id) {
        if (!subjectRepository.existsById(id)) {
            throw new ResourceNotFoundException("Subject not found with id: " + id);
        }
        subjectRepository.deleteById(id);
    }

    private void validateUniqueSubject(String name, Long currentId) {
        subjectRepository.findByNameIgnoreCase(name).ifPresent(found -> {
            if (currentId == null || !found.getId().equals(currentId)) {
                throw new DuplicateResourceException("Subject name already exists");
            }
        });
    }

    private Set<DepartmentEntity> getDepartmentsByIdsOrThrow(Set<Long> departmentIds) {
        Set<Long> requested = departmentIds == null ? Set.of() : new HashSet<>(departmentIds);
        if (requested.isEmpty()) {
            throw new ResourceNotFoundException("At least one department id is required");
        }

        List<DepartmentEntity> departments = departmentRepository.findAllById(requested);
        if (departments.size() != requested.size()) {
            Set<Long> foundIds = departments.stream().map(DepartmentEntity::getId).collect(Collectors.toSet());
            Set<Long> missingIds = requested.stream().filter(id -> !foundIds.contains(id)).collect(Collectors.toSet());
            throw new ResourceNotFoundException("Department ids not found: " + missingIds);
        }
        return new HashSet<>(departments);
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private SubjectResponseModel toResponse(SubjectEntity entity) {
        return new SubjectResponseModel(
                entity.getId(),
                entity.getName(),
                entity.getDepartments().stream().map(DepartmentEntity::getId).collect(Collectors.toSet()),
                entity.getLecture(),
                entity.getTutorial(),
                entity.getPractical());
    }
}
