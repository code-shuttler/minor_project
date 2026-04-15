package com.backend.models.room;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RoomRequestModel {

    @NotBlank(message = "Room name is required")
    @Size(max = 120, message = "Room name must be at most 120 characters")
    private String name;

    @NotNull(message = "Is lab is required")
    @JsonProperty("isLab")
    @JsonAlias({ "islab" })
    private Boolean isLab;

    @JsonProperty("department_id")
    @JsonAlias("departmentId")
    private Long departmentId;
}