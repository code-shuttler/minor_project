package com.backend.models.users;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.backend.exceptions.InvalidCredentialException;
import com.backend.exceptions.UserAlreadyExistsException;
import com.backend.models.jwt.JWTService;

@Service
public class UserService implements UserDetailsService {

    @Autowired
    UserRepository userRepository;

    @Autowired
    JWTService jwtService;

    @Autowired
    private PasswordEncoder passwordEncoder;

    // Method to load user details by username for authentication
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        UserEntity userEntity = userRepository.findByUsername(username);
        if (userEntity == null) {
            throw new UsernameNotFoundException("User not found with username: " + username);
        }
        String role = userEntity.getRole() != null ? userEntity.getRole().name() : UserRole.STUDENT.name();
        return new UserRequestModel(userEntity.getUsername(), userEntity.getPassword(), role);
    }

    // Method to save a new user
    public UserResponseModel saveUser(UserRequestModel user) {
        if (userRepository.existsByUsername(user.getUsername())) {
            throw new UserAlreadyExistsException("Username already exists");
        }
        UserEntity userEntity = new UserEntity();
        userEntity.setUsername(user.getUsername());
        userEntity.setPassword(passwordEncoder.encode(user.getPassword()));
        userEntity.setRole(UserRole.fromInput(user.getRole()));
        try {
            userRepository.save(userEntity);
        } catch (Exception e) {
            throw new UserAlreadyExistsException("Error saving user: " + e.getMessage());
        }
        return new UserResponseModel(userEntity.getUsername(), userEntity.getRole().name());
    }

    public UserEntity getUserByUsername(String username) {
        UserEntity userEntity = userRepository.findByUsername(username);
        if (userEntity == null) {
            throw new UsernameNotFoundException("User not found with username: " + username);
        }
        return userEntity;
    }

    // Method to verify user credentials and generate JWT token
    public String verifyUser(UserRequestModel user, AuthenticationManager authenticationManager) {
        if (user.getUsername() == null || user.getPassword() == null) {
            throw new InvalidCredentialException("Invalid credentials");
        }

        try {
            authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(user.getUsername(), user.getPassword()));
            return jwtService.generateToken(user.getUsername());
        } catch (BadCredentialsException ex) {
            throw new InvalidCredentialException("Invalid credentials");
        }
    }

}
