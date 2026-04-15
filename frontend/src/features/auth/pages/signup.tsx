import { Button } from "@/shared/components/ui/button";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import { Label } from "@/shared/components/ui/label";
import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import gradientBackgroundDark from "@/assets/images/gradient-background-dark.png";
import gradientBackgroundLight from "@/assets/images/gradient-background-light.png";
import { type ApiError } from "@/shared/api/http";
import { authContext } from "../authContext";

function Signup() {
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const navigate = useNavigate();
    const isDarkMode = typeof document !== "undefined" && document.documentElement.classList.contains("dark");

    const handleSignup = async (e: React.FormEvent) => {
        e.preventDefault()
        setError(null)
        setIsLoading(true)
        
        try {
            const response = await authContext.signup({ username, password })
            console.log("Signup successful:", response)
            navigate("/login")
        } catch (err) {
            const apiError = err as ApiError
            const errorMessage = (apiError.data ? apiError.data : (apiError.message ? apiError.message : "Signup failed. Please try again.")) as string;
            setError(errorMessage)
            console.error("Signup failed:", err)
        } finally {
            setIsLoading(false)
        }
    }

    return (
        <div
            className="min-h-screen flex items-center justify-center p-4"
            style={{
                backgroundImage: `url(${isDarkMode ? gradientBackgroundDark : gradientBackgroundLight})`,
                backgroundSize: "cover",
                backgroundPosition: "center",
                backgroundRepeat: "no-repeat",
            }}
        >
            <div
                className="absolute inset-0 opacity-0"
                style={{
                    background: "rgba(0, 0, 0, 0.15)",
                }}
            ></div>

            {/* Floating glass orbs for visual interest */}
            <div className="absolute inset-0 overflow-hidden pointer-events-none">
                <div
                    className="absolute top-1/4 left-1/4 w-32 h-32 rounded-full opacity-50 animate-pulse"
                    style={{
                        background: "rgba(255, 255, 255, 0.15)",
                        backdropFilter: "blur(20px) saturate(180%)",
                        border: "2px solid rgba(255, 255, 255, 0.3)",
                        boxShadow: "0 8px 32px rgba(255, 255, 255, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.4)",
                    }}
                ></div>
                <div
                    className="absolute top-3/4 right-1/4 w-24 h-24 rounded-full opacity-40 animate-pulse delay-1000"
                    style={{
                        background: "rgba(255, 255, 255, 0.15)",
                        backdropFilter: "blur(20px) saturate(180%)",
                        border: "2px solid rgba(255, 255, 255, 0.3)",
                        boxShadow: "0 8px 32px rgba(255, 255, 255, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.4)",
                    }}
                ></div>
                <div
                    className="absolute top-1/2 right-1/3 w-16 h-16 rounded-full opacity-45 animate-pulse delay-500"
                    style={{
                        background: "rgba(255, 255, 255, 0.15)",
                        backdropFilter: "blur(20px) saturate(180%)",
                        border: "2px solid rgba(255, 255, 255, 0.3)",
                        boxShadow: "0 8px 32px rgba(255, 255, 255, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.4)",
                    }}
                ></div>
            </div>

            <Card
                className="max-w-md hover-lift shadow-2xl relative z-10 opacity-100 w-[126%] mx-[0] border-transparent"
                style={{
                    background: "rgba(255, 255, 255, 0.25)",
                    backdropFilter: "blur(40px) saturate(250%)",
                    border: "1px solid rgba(255, 255, 255, 0.4)",
                    boxShadow:
                        "0 32px 80px rgba(0, 0, 0, 0.3), 0 16px 64px rgba(255, 255, 255, 0.2), inset 0 3px 0 rgba(255, 255, 255, 0.6), inset 0 -1px 0 rgba(255, 255, 255, 0.3)",
                }}
            >
                <CardHeader className="text-center space-y-2">
                    <CardTitle className="text-3xl font-bold font-sans text-card-foreground">Create Account</CardTitle>
                    <CardDescription className="text-card-foreground/70 font-sans">
                        Sign up to create your account
                    </CardDescription>
                </CardHeader>

                <CardContent className="space-y-6">
                    {error && (
                        <Alert variant="destructive">
                            <AlertDescription>{error}</AlertDescription>
                        </Alert>
                    )}
                    <form onSubmit={handleSignup} className="space-y-4">
                        <div className="space-y-2">
                            <Label htmlFor="username" className="text-sm font-medium text-card-foreground font-sans">
                                Username
                            </Label>
                            <Input
                                id="username"
                                type="text"
                                placeholder="Enter your username"
                                value={username}
                                onChange={(e) => setUsername(e.target.value)}
                                className="border-white/40 bg-white/10 placeholder:text-card-foreground/50 text-card-foreground py-3 focus:ring-2 focus:ring-blue-400 focus:border-blue-400 focus:bg-white/15 transition-all duration-200"
                                required
                            />
                        </div>

                        <div className="space-y-2">
                            <Label htmlFor="password" className="text-sm font-medium text-card-foreground font-sans">
                                Password
                            </Label>
                            <Input
                                id="password"
                                type="password"
                                placeholder="Enter your password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                className="border-white/40 bg-white/10 placeholder:text-card-foreground/50 text-card-foreground py-3 focus:ring-2 focus:ring-blue-400 focus:border-blue-400 focus:bg-white/15 transition-all duration-200"
                                required
                            />
                        </div>

                        <Button
                            type="submit"
                            className="w-full ripple-effect hover-lift font-sans font-bold py-5 transition-all duration-300"
                            style={{ backgroundColor: "#0C115B", color: "white" }}
                            disabled={isLoading}
                        >
                            {isLoading ? "Signing Up..." : "Sign Up"}
                        </Button>
                    </form>

                    <div className="text-center text-sm text-card-foreground/80 font-sans">
                        Already have an account? {" "}
                        <Link
                            to="/login"
                            className="font-semibold text-card-foreground hover:underline"
                        >
                            Sign in
                        </Link>
                    </div>
                </CardContent>
            </Card>
        </div>
    )
}

export default Signup