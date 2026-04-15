package com.backend.models.department;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.backend.exceptions.DuplicateResourceException;
import com.backend.exceptions.ResourceNotFoundException;

@Service
public class DepartmentService {

    @Autowired
    private DepartmentRepository departmentRepository;

    public List<DepartmentResponseModel> getAllDepartments() {
        return departmentRepository.findAll().stream().map(this::toResponse).toList();
    }

    public DepartmentResponseModel getDepartmentById(Long id) {
        DepartmentEntity entity = departmentRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Department not found with id: " + id));
        return toResponse(entity);
    }

    public DepartmentResponseModel createDepartment(DepartmentRequestModel request) {
        String normalizedName = normalize(request.getName());
        String normalizedAbbreviation = normalize(request.getAbbreviation()).toUpperCase();

        validateUniqueDepartment(normalizedName, normalizedAbbreviation, null);

        DepartmentEntity entity = new DepartmentEntity();
        entity.setName(normalizedName);
        entity.setAbbreviation(normalizedAbbreviation);

        try {
            DepartmentEntity saved = departmentRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Department with same name or abbreviation already exists");
        }
    }

    public DepartmentResponseModel updateDepartment(Long id, DepartmentRequestModel request) {
        DepartmentEntity existing = departmentRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Department not found with id: " + id));

        String normalizedName = normalize(request.getName());
        String normalizedAbbreviation = normalize(request.getAbbreviation()).toUpperCase();

        validateUniqueDepartment(normalizedName, normalizedAbbreviation, id);

        existing.setName(normalizedName);
        existing.setAbbreviation(normalizedAbbreviation);

        try {
            DepartmentEntity saved = departmentRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Department with same name or abbreviation already exists");
        }
    }

    public void deleteDepartment(Long id) {
        if (!departmentRepository.existsById(id)) {
            throw new ResourceNotFoundException("Department not found with id: " + id);
        }
        departmentRepository.deleteById(id);
    }

    private void validateUniqueDepartment(String name, String abbreviation, Long currentId) {
        departmentRepository.findByNameIgnoreCase(name).ifPresent(found -> {
            if (currentId == null || !found.getId().equals(currentId)) {
                throw new DuplicateResourceException("Department name already exists");
            }
        });

        departmentRepository.findByAbbreviationIgnoreCase(abbreviation).ifPresent(found -> {
            if (currentId == null || !found.getId().equals(currentId)) {
                throw new DuplicateResourceException("Department abbreviation already exists");
            }
        });
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private DepartmentResponseModel toResponse(DepartmentEntity entity) {
        return new DepartmentResponseModel(entity.getId(), entity.getName(), entity.getAbbreviation());
    }
}
