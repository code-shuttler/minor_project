package com.backend.models.users;

public enum UserRole {
    ADMIN,
    TEACHER,
    STUDENT;

    public static UserRole fromInput(String input) {
        if (input == null || input.isBlank()) {
            return STUDENT;
        }

        String normalized = input.trim().toUpperCase();
        if ("STUDENB".equals(normalized)) {
            return STUDENT;
        }

        return switch (normalized) {
            case "ADMIN" -> ADMIN;
            case "TEACHER" -> TEACHER;
            case "STUDENT" -> STUDENT;
            default -> throw new IllegalArgumentException("Invalid role: " + input);
        };
    }

    public String toAuthority() {
        return "ROLE_" + name();
    }
}
