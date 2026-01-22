import functions_framework
from src.q1_time import q1_time
from src.q1_memory import q1_memory
from src.q2_time import q2_time
from src.q2_memory import q2_memory
from src.q3_time import q3_time
from src.q3_memory import q3_memory
import json
import os


@functions_framework.http
def entrypoint(request):
    """Entrypoint para Cloud Function HTTP/PubSub."""
    request_json = request.get_json(silent=True)

    # Caso 1: Evento Pub/Sub (GCS Notification)
    if request_json and "message" in request_json:
        # Extraer datos del mensaje Pub/Sub si es necesario
        # Por simplicidad, asumimos que procesamos el archivo por defecto
        file_path = os.environ.get("INPUT_FILE_PATH")
        result = q1_time(file_path)
        return json.dumps({"status": "batch_processed", "q1": str(result)}), 200

    # Caso 2: Request HTTP On-demand
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
    return json.dumps({"question": q, "strategy": strategy, "result": str(result)}), 200
