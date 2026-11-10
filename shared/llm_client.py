"""
llm_client.py — Cliente LLM compartido (Google Gemini) para step5 y step6.
==========================================================================

Variables de entorno (.env):
    GOOGLE_API_KEY   API key de Gemini (formato "AQ..." o "AIza..."). También se acepta GEMINI_API_KEY.
    GEMINI_MODEL     Modelo a usar. Por defecto: gemini-3.5-flash-lite.

Verificar que la clave funciona (no escribe nada en la BD):
    python shared/llm_client.py --probar
"""

import asyncio
import os
import sys

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "gemini-3.5-flash-lite"

MISSING_KEY_MSG = (
    "ERROR: no se encontró la API key de Gemini. Define GOOGLE_API_KEY en el .env "
    "(o GEMINI_API_KEY). Para validarla: python shared/llm_client.py --probar"
)


def get_api_key() -> str | None:
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")


def get_model() -> str:
    return os.getenv("GEMINI_MODEL") or DEFAULT_MODEL


class GeminiClient:
    """Envoltorio async mínimo sobre google-genai: una llamada = un texto de respuesta."""

    def __init__(self, api_key: str | None = None, model: str | None = None):
        # Import diferido: los demás steps no necesitan el SDK instalado.
        from google import genai
        from google.genai import types

        self._types = types
        self.model = model or get_model()
        self._client = genai.Client(api_key=api_key or get_api_key())

    async def generate(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int,
        temperature: float,
        json_output: bool = False,
    ) -> str:
        """
        Retorna el texto de la respuesta (sin espacios sobrantes).
        Lanza RuntimeError si Gemini responde vacío (bloqueo de seguridad o tokens agotados),
        para que el reintento del step lo trate como cualquier otro error.

        Nota: los modelos Gemini 3 "piensan" antes de responder y esos tokens cuentan contra
        max_tokens, por eso los steps usan límites holgados; la longitud real la controla el prompt.
        """
        config = self._types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=temperature,
            response_mime_type="application/json" if json_output else None,
        )
        response = await self._client.aio.models.generate_content(
            model=self.model, contents=user, config=config
        )

        text = (response.text or "").strip()
        if not text:
            reason = None
            candidates = getattr(response, "candidates", None) or []
            if candidates:
                reason = getattr(candidates[0], "finish_reason", None)
            feedback = getattr(response, "prompt_feedback", None)
            raise RuntimeError(f"Respuesta vacía de Gemini (finish_reason={reason}, feedback={feedback})")
        return text


def _probar() -> int:
    key = get_api_key()
    if not key:
        print(MISSING_KEY_MSG)
        return 1

    # Solo se muestra el tipo de clave, nunca su valor
    kind = "AQ." if key.startswith("AQ.") else ("AIza" if key.startswith("AIza") else "desconocido")
    print(f"Modelo: {get_model()} | formato de clave: {kind} | largo: {len(key)}")

    async def _go():
        client = GeminiClient()
        return await client.generate(
            "Responde en español, en una sola frase corta.",
            "Di 'conexión con Gemini OK' y nada más.",
            max_tokens=256,
            temperature=0.0,
        )

    try:
        print("Respuesta:", asyncio.run(_go()))
        return 0
    except Exception as e:
        print(f"FALLO: {e}")
        if "ACCESS_TOKEN_TYPE_UNSUPPORTED" in str(e) or "401" in str(e):
            print(
                "\nPistas: 1) copia la clave completa, sin comillas ni espacios; "
                "2) prueba otro modelo vigente con GEMINI_MODEL (ej: gemini-3.1-flash-lite); "
                "3) revisa que el proyecto de la clave tenga la Gemini API habilitada."
            )
        return 1


if __name__ == "__main__":
    if "--probar" in sys.argv:
        sys.exit(_probar())
    print(__doc__)
