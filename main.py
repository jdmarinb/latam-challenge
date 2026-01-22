import functions_framework
from src.q1_time import q1_time
from src.q1_memory import q1_memory
from src.q2_time import q2_time
from src.q2_memory import q2_memory
from src.q3_time import q3_time
from src.q3_memory import q3_memory
import json
import base64
from google.cloud import storage


def _write_to_gcs(bucket_name, blob_name, data):
    """Escribe datos en un bucket de GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="application/json")
    return f"gs://{bucket_name}/{blob_name}"


@functions_framework.http
def entrypoint(request):
    """Entrypoint universal para Cloud Function (HTTP y Pub/Sub)."""
    request_json = request.get_json(silent=True)

    # Caso 1: Evento Pub/Sub (GCS Notification vía Eventarc)
    if request_json and "message" in request_json:
        try:
            pubsub_message = request_json["message"]
            if "data" in pubsub_message:
                data = json.loads(
                    base64.b64decode(pubsub_message["data"]).decode("utf-8")
                )
                bucket = data.get("bucket")
                name = data.get("name")
                if bucket and name:
                    file_path = f"gs://{bucket}/{name}"

                    # Ejecutar procesamiento
                    result = q1_time(file_path)

                    # Persistir resultado en la carpeta /output
                    output_name = name.replace("input/", "output/result_")
                    output_path = _write_to_gcs(bucket, output_name, json.dumps(result))

                    return json.dumps(
                        {
                            "status": "success",
                            "trigger": "pubsub",
                            "input": file_path,
                            "output": output_path,
                        }
                    ), 200

            return json.dumps(
                {"status": "error", "message": "Invalid Pub/Sub payload"}
            ), 400
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)}), 500

    # Caso 2: Request HTTP On-demand
    q = request.args.get("q", "q1")
    strategy = request.args.get("strategy", "time")
    file_path = request.args.get("file")

    if not file_path:
        return json.dumps(
            {"status": "error", "message": "Missing required parameter: file"}
        ), 400

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

    try:
        result = func(file_path)
        return json.dumps(
            {
                "question": q,
                "strategy": strategy,
                "file": file_path,
                "result": str(result),
            }
        ), 200
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
