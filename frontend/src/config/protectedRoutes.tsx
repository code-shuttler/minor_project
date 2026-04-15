import { useEffect, useState } from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { authContext } from "@/features/auth/authContext";

function ProtectedRoutes() {
  const [isChecking, setIsChecking] = useState(true);
  const [isAllowed, setIsAllowed] = useState(false);
  const location = useLocation();

  useEffect(() => {
    let isMounted = true;

    const checkAuth = async () => {
      const authenticated = await authContext.isAuthenticated();
      if (isMounted) {
        setIsAllowed(authenticated);
        setIsChecking(false);
      }
    };

    void checkAuth();

    return () => {
      isMounted = false;
    };
  }, []);

  if (isChecking) {
    return <div>Checking session...</div>;
  }

  if (!isAllowed) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }

  return <Outlet />;
}

export default ProtectedRoutes;