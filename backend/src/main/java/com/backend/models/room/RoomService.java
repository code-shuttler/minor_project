package com.backend.models.room;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.backend.exceptions.DuplicateResourceException;
import com.backend.exceptions.ResourceNotFoundException;
import com.backend.models.department.DepartmentEntity;
import com.backend.models.department.DepartmentRepository;

@Service
public class RoomService {

    @Autowired
    private RoomRepository roomRepository;

    @Autowired
    private DepartmentRepository departmentRepository;

    public List<RoomResponseModel> getAllRooms() {
        return roomRepository.findAll().stream().map(this::toResponse).toList();
    }

    public RoomResponseModel getRoomById(Long id) {
        RoomEntity entity = roomRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Room not found with id: " + id));
        return toResponse(entity);
    }

    public RoomResponseModel createRoom(RoomRequestModel request) {
        String normalizedName = normalize(request.getName());
        validateUniqueRoom(normalizedName, null);

        RoomEntity entity = new RoomEntity();
        entity.setName(normalizedName);
        entity.setLab(Boolean.TRUE.equals(request.getIsLab()));
        entity.setDepartment(getDepartmentOrNull(request.getDepartmentId()));

        try {
            RoomEntity saved = roomRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Room with same name already exists");
        }
    }

    public RoomResponseModel updateRoom(Long id, RoomRequestModel request) {
        RoomEntity existing = roomRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Room not found with id: " + id));

        String normalizedName = normalize(request.getName());
        validateUniqueRoom(normalizedName, id);

        existing.setName(normalizedName);
        existing.setLab(Boolean.TRUE.equals(request.getIsLab()));
        existing.setDepartment(getDepartmentOrNull(request.getDepartmentId()));

        try {
            RoomEntity saved = roomRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Room with same name already exists");
        }
    }

    public void deleteRoom(Long id) {
        if (!roomRepository.existsById(id)) {
            throw new ResourceNotFoundException("Room not found with id: " + id);
        }
        roomRepository.deleteById(id);
    }

    private DepartmentEntity getDepartmentOrNull(Long departmentId) {
        if (departmentId == null) {
            return null;
        }
        return departmentRepository.findById(departmentId)
                .orElseThrow(() -> new ResourceNotFoundException("Department not found with id: " + departmentId));
    }

    private void validateUniqueRoom(String name, Long currentId) {
        roomRepository.findByNameIgnoreCase(name).ifPresent(found -> {
            if (currentId == null || !found.getId().equals(currentId)) {
                throw new DuplicateResourceException("Room name already exists");
            }
        });
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private RoomResponseModel toResponse(RoomEntity entity) {
        return new RoomResponseModel(
                entity.getId(),
                entity.getName(),
                entity.isLab(),
                entity.getDepartment() == null ? null : entity.getDepartment().getId());
    }
}