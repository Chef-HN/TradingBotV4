"""
Default auth translation strings for py-auth-kit.

Apps import AUTH_TRANSLATIONS and merge into their own translation dict:

    from auth_kit.i18n import AUTH_TRANSLATIONS

    TRANSLATIONS = {
        "en": {**AUTH_TRANSLATIONS["en"], ...app_strings...},
        "es": {**AUTH_TRANSLATIONS["es"], ...app_strings...},
    }

Apps can override any key by placing it after the spread.
"""

AUTH_TRANSLATIONS: dict[str, dict[str, str]] = {
    "en": {
        "auth.login_title": "Log In",
        "auth.register_title": "Create Account",
        "auth.email": "Email",
        "auth.password": "Password",
        "auth.display_name": "Display Name",
        "auth.preferred_language": "Preferred Language",
        "auth.no_account": "Don't have an account?",
        "auth.has_account": "Already have an account?",
        "auth.login_button": "Log In",
        "auth.register_button": "Register",
        "auth.error_invalid_credentials": "Invalid email or password.",
        "auth.error_email_exists": "An account with this email already exists.",
        "auth.error_password_mismatch": "Passwords do not match.",
        "auth.error_email_not_found": "No account found with that email address.",
        "auth.create_account_prompt": "Create an account",
        "auth.verify_otp_title": "Verify your email",
        "auth.verify_otp_subtitle": "We sent a 6-digit code to",
        "auth.otp_code_label": "Verification code",
        "auth.verify_button": "Verify and create account",
        "auth.resend_otp": "Resend code",
        "auth.otp_invalid": "Invalid or expired code. Please try again.",
        "auth.otp_sent": "A new code has been sent to your email.",
        "auth.forgot_password_title": "Forgot your password?",
        "auth.forgot_password_subtitle": "Enter your email and we'll send you a verification code.",
        "auth.forgot_password_link": "Forgot your password?",
        "auth.send_reset_code": "Send reset code",
        "auth.reset_password_title": "Reset your password",
        "auth.reset_password_subtitle": "Enter the code sent to",
        "auth.new_password": "New password",
        "auth.confirm_password": "Confirm new password",
        "auth.reset_password_btn": "Reset password",
        "auth.back_to_login": "Back to login",
        "auth.passwords_mismatch": "Passwords do not match.",
    },
    "es": {
        "auth.login_title": "Iniciar Sesión",
        "auth.register_title": "Crear Cuenta",
        "auth.email": "Correo electrónico",
        "auth.password": "Contraseña",
        "auth.display_name": "Nombre para mostrar",
        "auth.preferred_language": "Idioma preferido",
        "auth.no_account": "¿No tienes una cuenta?",
        "auth.has_account": "¿Ya tienes una cuenta?",
        "auth.login_button": "Iniciar Sesión",
        "auth.register_button": "Registrarse",
        "auth.error_invalid_credentials": "Correo electrónico o contraseña inválidos.",
        "auth.error_email_exists": "Ya existe una cuenta con este correo electrónico.",
        "auth.error_password_mismatch": "Las contraseñas no coinciden.",
        "auth.error_email_not_found": "No encontramos ninguna cuenta con ese correo electrónico.",
        "auth.create_account_prompt": "Crear una cuenta",
        "auth.verify_otp_title": "Verifica tu correo",
        "auth.verify_otp_subtitle": "Enviamos un código de 6 dígitos a",
        "auth.otp_code_label": "Código de verificación",
        "auth.verify_button": "Verificar y crear cuenta",
        "auth.resend_otp": "Reenviar código",
        "auth.otp_invalid": "Código inválido o expirado. Por favor, inténtalo de nuevo.",
        "auth.otp_sent": "Se ha enviado un nuevo código a tu correo electrónico.",
        "auth.forgot_password_title": "¿Olvidaste tu contraseña?",
        "auth.forgot_password_subtitle": "Ingresa tu correo y te enviaremos un código de verificación.",
        "auth.forgot_password_link": "¿Olvidaste tu contraseña?",
        "auth.send_reset_code": "Enviar código",
        "auth.reset_password_title": "Restablecer contraseña",
        "auth.reset_password_subtitle": "Ingresa el código enviado a",
        "auth.new_password": "Nueva contraseña",
        "auth.confirm_password": "Confirmar nueva contraseña",
        "auth.reset_password_btn": "Restablecer contraseña",
        "auth.back_to_login": "Volver al inicio de sesión",
        "auth.passwords_mismatch": "Las contraseñas no coinciden.",
    },
}
