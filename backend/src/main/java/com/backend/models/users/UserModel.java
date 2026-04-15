package com.backend.models.users;

import java.util.Collection;
import java.util.Collections;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
class UserRequestModel implements UserDetails{
    private String username;
    private String password;
    private String role;

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        UserRole resolvedRole = UserRole.fromInput(role);
        return Collections.singleton(new SimpleGrantedAuthority(resolvedRole.toAuthority()));
    }
}

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
class UserResponseModel {
    private String username;
    private String role;
}
