import { get, post } from "@/shared/api/http";

export type AuthCredentials = {
	username: string;
	password: string;
	role?: "admin" | "teacher" | "student";
};

export type AuthUser = {
	id?: string | number;
	username: string;
	roles?: string[];
};

export type AuthProfileResponse = {
	name: string | null;
	role?: "admin" | "teacher" | "student" | null;
	authorities?: string[];
};

export type SignupResponse = {
	username: string;
	role?: string;
};

class AuthContext {
	async login(credentials: AuthCredentials): Promise<string> {
		return post<string, AuthCredentials>("/auth/login", credentials);
	}

	async signup(credentials: AuthCredentials): Promise<SignupResponse> {
		return post<SignupResponse, AuthCredentials>("/auth/signup", credentials);
	}

	async getCurrentUser(): Promise<AuthUser | null> {
		const profile = await get<AuthProfileResponse>("/auth/me");
		if (!profile?.name) {
			return null;
		}

		return {
			username: profile.name,
			roles: profile.authorities ?? (profile.role ? [`ROLE_${profile.role.toUpperCase()}`] : []),
		};
	}

	async isAuthenticated(): Promise<boolean> {
		try {
			const profile = await this.getCurrentUser();
			return !!profile;
		} catch {
			return false;
		}
	}

	async logout(): Promise<void> {
		await post<string, Record<string, never>>("/auth/logout", {});
	}
}

export const authContext = new AuthContext();

export default AuthContext;
