import functions_framework
from src.q1_time import q1_time
from src.q1_memory import q1_memory
from src.q2_time import q2_time
from src.q2_memory import q2_memory
from src.q3_time import q3_time
from src.q3_memory import q3_memory
import json
import os
import base64


@functions_framework.http
def entrypoint(request):
    """Entrypoint universal para Cloud Function (HTTP y Pub/Sub)."""
    request_json = request.get_json(silent=True)

    # Caso 1: Evento Pub/Sub (GCS Notification vía Eventarc)
    if request_json and "message" in request_json:
        # Extraer metadatos de GCS del mensaje
        # GCS envía los datos en el campo 'data' del mensaje en Base64
        try:
            pubsub_message = request_json["message"]
            if "data" in pubsub_message:
                data = json.loads(
                    base64.b64decode(pubsub_message["data"]).decode("utf-8")
                )
                # GCS Notification format: data contiene 'bucket' y 'name'
                bucket = data.get("bucket")
                name = data.get("name")
                if bucket and name:
                    file_path = f"gs://{bucket}/{name}"
                else:
                    file_path = os.environ.get("INPUT_FILE_PATH")
            else:
                file_path = os.environ.get("INPUT_FILE_PATH")
        except Exception:
            file_path = os.environ.get("INPUT_FILE_PATH")

        # Por diseño, los eventos batch ejecutan Q1 Time
        result = q1_time(file_path)
        return json.dumps(
            {"status": "batch_processed", "file": file_path, "q1": str(result)}
        ), 200

    # Caso 2: Request HTTP On-demand (Dashboard/Manual)
    q = request.args.get("q", "q1")
    strategy = request.args.get("strategy", "time")
    file_path = request.args.get("file", os.environ.get("INPUT_FILE_PATH"))

    funcs = {
        ("q1", "time"): q1_time,
        ("q1", "memory"): q1_memory,
        ("q2", "time"): q2_time,
        ("q2", "memory"): q2_memory,
        ("q3", "time"): q3_time,
        ("q3", "memory"): q3_memory,
    }

    func = funcs.get((q, strategy))
    if not func:
        return "Invalid question or strategy", 400

    result = func(file_path)
    return json.dumps(
        {"question": q, "strategy": strategy, "file": file_path, "result": str(result)}
    ), 200
