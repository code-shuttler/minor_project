package com.backend.models.users;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Value;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/auth")
public class UserController {

    @Value("${app.auth.cookie.name:token}")
    private String authCookieName;

    @Value("${app.auth.cookie.secure:false}")
    private boolean forceSecureCookie;

    @Value("${app.auth.cookie.same-site:Lax}")
    private String sameSite;

    @Value("${app.auth.jwt-expiration-ms:900000}")
    private long jwtExpirationMs;

    @Autowired
    private UserService userService;

    @Autowired
    private AuthenticationManager authenticationManager;

    @PostMapping("/signup")
    public ResponseEntity<UserResponseModel> signUpUser(@RequestBody UserRequestModel user) {
        return ResponseEntity.ok(userService.saveUser(user));
    }

    @PostMapping("/login")
    public ResponseEntity<String> loginUser(@RequestBody UserRequestModel user, HttpServletRequest request) {
        String token = userService.verifyUser(user, authenticationManager);
        boolean useSecureCookie = forceSecureCookie || request.isSecure();

        ResponseCookie cookie = ResponseCookie.from(authCookieName, token)
                .httpOnly(true)
                .secure(useSecureCookie)
                .sameSite(sameSite)
                .path("/")
                .maxAge(jwtExpirationMs / 1000)
                .build();

        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, cookie.toString())
                .body("Login successful");
    }

    @PostMapping("/logout")
    public ResponseEntity<String> logoutUser(HttpServletRequest request) {
        boolean useSecureCookie = forceSecureCookie || request.isSecure();

        ResponseCookie expiredCookie = ResponseCookie.from(authCookieName, "")
                .httpOnly(true)
                .secure(useSecureCookie)
                .sameSite(sameSite)
                .path("/")
                .maxAge(0)
                .build();

        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, expiredCookie.toString())
                .body("Logout successful");
    }

    @GetMapping("/me")
    public ResponseEntity<Map<String, Object>> getLoggedInUser(Principal principal) {
        Map<String, Object> profile = new HashMap<>();
        if (principal == null) {
            profile.put("name", null);
            profile.put("role", null);
            profile.put("authorities", List.of());
            return ResponseEntity.ok(profile);
        }

        UserEntity userEntity = userService.getUserByUsername(principal.getName());
        UserRole role = userEntity.getRole() != null ? userEntity.getRole() : UserRole.STUDENT;

        profile.put("name", userEntity.getUsername());
        profile.put("role", role.name().toLowerCase());
        profile.put("authorities", List.of(role.toAuthority()));
        return ResponseEntity.ok(profile);
    }

}
