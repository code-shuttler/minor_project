package com.backend.models.room;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RoomRepository extends JpaRepository<RoomEntity, Long> {
    boolean existsByNameIgnoreCase(String name);

    Optional<RoomEntity> findByNameIgnoreCase(String name);
}