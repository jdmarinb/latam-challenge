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
import datetime


def _serializable_result(result):
    """Convierte resultados de Polars a JSON serializable."""
    if not isinstance(result, list):
        return str(result)

    clean_result = []
    for item in result:
        # Convertir tuplas a listas y fechas a string
        clean_item = []
        for val in item:
            if isinstance(val, (datetime.date, datetime.datetime)):
                clean_item.append(val.isoformat())
            else:
                clean_item.append(val)
        clean_result.append(clean_item)
    return clean_result


def _write_to_gcs(bucket_name, blob_name, data_str):
    """Escribe datos en un bucket de GCS."""
    print(f"[GCS] Intentando subir a: gs://{bucket_name}/{blob_name}")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data_str, content_type="application/json")
        print(f"[GCS] Subida exitosa: gs://{bucket_name}/{blob_name}")
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        print(f"[GCS] ERROR en subida: {str(e)}")
        raise e


@functions_framework.http
def entrypoint(request):
    """Entrypoint universal para Cloud Function (HTTP y Pub/Sub)."""
    request_json = request.get_json(silent=True)

    # Caso 1: Evento Pub/Sub (GCS Notification vía Eventarc)
    if request_json and "message" in request_json:
        print("[TRIGGER] Detectado evento Pub/Sub")
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
                    print(f"[BATCH] Procesando archivo: {file_path}")

                    # Ejecutar procesamiento
                    result = q1_time(file_path)

                    # Persistir resultado
                    output_name = name.replace("input/", "output/")
                    if output_name == name:
                        output_name = f"output/{name.split('/')[-1]}"
                    serializable = _serializable_result(result)
                    output_path = _write_to_gcs(
                        bucket, output_name, json.dumps(serializable)
                    )

                    return json.dumps(
                        {
                            "status": "success",
                            "trigger": "pubsub",
                            "input": file_path,
                            "output": output_path,
                        }
                    ), 200

            print("[ERROR] Payload de Pub/Sub inválido")
            return json.dumps(
                {"status": "error", "message": "Invalid Pub/Sub payload"}
            ), 400
        except Exception as e:
            print(f"[ERROR FATAL] {str(e)}")
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
                "result": _serializable_result(result),
            }
        ), 200
    except Exception as e:
        print(f"[HTTP ERROR] {str(e)}")
        return json.dumps({"status": "error", "message": str(e)}), 500
