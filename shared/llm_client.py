"""
llm_client.py — Cliente LLM compartido (Google Gemini) para step5 y step6.
==========================================================================

Variables de entorno (.env):
    GOOGLE_API_KEY   API key de Gemini (formato "AQ..." o "AIza..."). También se acepta GEMINI_API_KEY.
    GEMINI_MODEL     Modelo a usar. Por defecto: gemini-3.5-flash-lite.

Logs (para revisar si el LLM funciona bien):
    logs/llm/<step>_<YYYY-MM-DD_HH-MM-SS>.log
    Una línea por llamada (latencia, tokens, finish_reason, respuesta), cada error/reintento
    y un resumen final. La clave API nunca se escribe en los logs.

Verificar que la clave funciona (no escribe nada en la BD):
    python shared/llm_client.py --probar
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "gemini-3.5-flash-lite"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs", "llm")
RESPONSE_LOG_CHARS = 400   # máximo de caracteres de la respuesta que se guarda por línea

MISSING_KEY_MSG = (
    "ERROR: no se encontró la API key de Gemini. Define GOOGLE_API_KEY en el .env "
    "(o GEMINI_API_KEY). Para validarla: python shared/llm_client.py --probar"
)


def get_api_key() -> str | None:
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")


def get_model() -> str:
    return os.getenv("GEMINI_MODEL") or DEFAULT_MODEL


def setup_llm_logger(step: str) -> tuple[logging.Logger, str]:
    """
    Crea un logger que escribe en logs/llm/<step>_<timestamp>.log (UTF-8).
    Retorna (logger, ruta_del_archivo). Cada ejecución genera su propio archivo.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(LOG_DIR, f"{step}_{stamp}.log")

    logger = logging.getLogger(f"llm.{step}.{stamp}")
    logger.setLevel(logging.INFO)
    logger.propagate = False   # no duplicar en el root logger
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    return logger, path


def _null_logger() -> logging.Logger:
    logger = logging.getLogger("llm.null")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return logger


def _one_line(text: str, limit: int = RESPONSE_LOG_CHARS) -> str:
    flat = " ".join(str(text).split())
    return flat if len(flat) <= limit else flat[:limit] + "…"


class GeminiClient:
    """Envoltorio async mínimo sobre google-genai: una llamada = un texto de respuesta."""

    def __init__(self, api_key: str | None = None, model: str | None = None,
                 logger: logging.Logger | None = None):
        # Import diferido: los demás steps no necesitan el SDK instalado.
        from google import genai
        from google.genai import types

        self._types = types
        self.model = model or get_model()
        self.logger = logger or _null_logger()
        self._client = genai.Client(api_key=api_key or get_api_key())
        # Contadores acumulados para el resumen final
        self.stats = {"llamadas": 0, "ok": 0, "errores": 0,
                      "tokens_entrada": 0, "tokens_salida": 0, "tokens_pensando": 0,
                      "latencia_total": 0.0}

    async def generate(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int,
        temperature: float,
        json_output: bool = False,
        tag: str = "",
    ) -> str:
        """
        Retorna el texto de la respuesta (sin espacios sobrantes).
        Lanza RuntimeError si Gemini responde vacío (bloqueo de seguridad o tokens agotados),
        para que el reintento del step lo trate como cualquier otro error.
        `tag` identifica la llamada en el log (ej: "id=123").

        Nota: los modelos Gemini 3 "piensan" antes de responder y esos tokens cuentan contra
        max_tokens, por eso los steps usan límites holgados; la longitud real la controla el prompt.
        """
        config = self._types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=temperature,
            response_mime_type="application/json" if json_output else None,
        )

        self.stats["llamadas"] += 1
        t0 = time.perf_counter()
        try:
            response = await self._client.aio.models.generate_content(
                model=self.model, contents=user, config=config
            )
        except Exception as e:
            elapsed = time.perf_counter() - t0
            self.stats["errores"] += 1
            self.stats["latencia_total"] += elapsed
            self.logger.error(f"ERROR {tag} latencia={elapsed:.2f}s {type(e).__name__}: {_one_line(e, 300)}")
            raise

        elapsed = time.perf_counter() - t0
        self.stats["latencia_total"] += elapsed

        usage = getattr(response, "usage_metadata", None)
        t_in = getattr(usage, "prompt_token_count", None) or 0
        t_out = getattr(usage, "candidates_token_count", None) or 0
        t_think = getattr(usage, "thoughts_token_count", None) or 0
        self.stats["tokens_entrada"] += t_in
        self.stats["tokens_salida"] += t_out
        self.stats["tokens_pensando"] += t_think

        candidates = getattr(response, "candidates", None) or []
        finish = getattr(candidates[0], "finish_reason", None) if candidates else None
        finish = getattr(finish, "name", finish)   # enum -> "STOP" / "MAX_TOKENS" / "SAFETY"

        text = (response.text or "").strip()
        meta = (f"latencia={elapsed:.2f}s tokens(in/out/pensando)={t_in}/{t_out}/{t_think} "
                f"finish={finish}")

        if not text:
            self.stats["errores"] += 1
            feedback = getattr(response, "prompt_feedback", None)
            self.logger.warning(f"VACIA {tag} {meta} feedback={feedback}")
            raise RuntimeError(f"Respuesta vacía de Gemini (finish_reason={finish}, feedback={feedback})")

        if finish not in (None, "STOP"):
            # Texto cortado o filtrado (ej: MAX_TOKENS): NO se acepta, para no guardar
            # descripciones a medias en la BD. El reintento del step lo vuelve a pedir.
            self.stats["errores"] += 1
            self.logger.warning(f"INCOMPLETA {tag} {meta} resp=\"{_one_line(text)}\"")
            raise RuntimeError(f"Respuesta incompleta de Gemini (finish_reason={finish})")

        self.logger.info(f"OK {tag} {meta} resp=\"{_one_line(text)}\"")

        self.stats["ok"] += 1
        return text

    def log_summary(self, extra: str = ""):
        """Escribe el resumen de la corrida en el log (llamar al terminar el step)."""
        s = self.stats
        n = s["llamadas"] or 1
        self.logger.info(
            "RESUMEN llamadas=%d ok=%d errores=%d latencia_media=%.2fs "
            "tokens_entrada=%d tokens_salida=%d tokens_pensando=%d %s",
            s["llamadas"], s["ok"], s["errores"], s["latencia_total"] / n,
            s["tokens_entrada"], s["tokens_salida"], s["tokens_pensando"], extra,
        )


def _probar() -> int:
    key = get_api_key()
    if not key:
        print(MISSING_KEY_MSG)
        return 1

    # Solo se muestra el tipo de clave, nunca su valor
    kind = "AQ." if key.startswith("AQ.") else ("AIza" if key.startswith("AIza") else "desconocido")
    print(f"Modelo: {get_model()} | formato de clave: {kind} | largo: {len(key)}")

    logger, log_path = setup_llm_logger("probar")

    async def _go():
        client = GeminiClient(logger=logger)
        return await client.generate(
            "Responde en español, en una sola frase corta.",
            "Di 'conexión con Gemini OK' y nada más.",
            max_tokens=256,
            temperature=0.0,
            tag="prueba",
        )

    try:
        print("Respuesta:", asyncio.run(_go()))
        print(f"Log: {log_path}")
        return 0
    except Exception as e:
        print(f"FALLO: {e}")
        print(f"Log: {log_path}")
        if "API_KEY_SERVICE_BLOCKED" in str(e):
            print(
                "\nLa clave es válida pero tiene restricciones de API: en Google Cloud Console > "
                "APIs y servicios > Credenciales > (tu clave) > 'Restricciones de API', permite "
                "'Generative Language API' (o quita la restricción). Verifica también que esa API "
                "esté habilitada en el proyecto de la clave."
            )
        elif "ACCESS_TOKEN_TYPE_UNSUPPORTED" in str(e) or "401" in str(e):
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
