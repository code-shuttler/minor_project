import { useEffect, useState } from "react";
import { authContext, type AuthUser } from "@/features/auth/authContext";
import { useNavigate } from "react-router-dom";


function Home() {
    const [currentUser, setCurrentUser] = useState<AuthUser | null>(null);
    const navigate = useNavigate();


    useEffect(() => {
        const loadCurrentUser = async () => {
            const user = await authContext.getCurrentUser();
            setCurrentUser(user);
        };

        void loadCurrentUser();
    }, []);

    const handleLogout = async () => {
        await authContext.logout();
        navigate('/login', { replace: true });
    };

    return (

        <div>
            <h1>Home</h1>
            <p>Logged in as: {currentUser?.username ?? 'Unknown user'}</p>
            <p>Roles: {currentUser?.roles?.join(', ') ?? 'No roles'}</p>
            <button type="button" onClick={handleLogout}>Logout</button>
        </div>
    );
}

export default Home;