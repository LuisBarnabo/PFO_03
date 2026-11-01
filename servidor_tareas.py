import socket
import threading
import queue
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor

HOST = '0.0.0.0'
PORT = 5000
NUM_WORKERS = 5
TAMANO_COLA = 100

# ------------------------- Cola de tareas -------------------------
cola_tareas = queue.Queue(maxsize=TAMANO_COLA)

# Diccionario de resultados pendientes
resultados_pendientes = {}
lock_resultados = threading.Lock()

# Bandera de apagado ordenado
apagando = threading.Event()

# Carpeta que simula el "bucket" de S3 (almacenamiento tipo objeto).
BUCKET_DIR = "resultados"
os.makedirs(BUCKET_DIR, exist_ok=True)


# ------------------------- Almacenamiento tipo S3 -------------------------
def guardar_en_bucket(nombre_objeto, contenido):
    """
    Simula el almacenamiento en S3: guarda el resultado como un objeto
    (archivo JSON) dentro del 'bucket' local.
    En producción sería boto3.client('s3').put_object(...).
    """
    ruta = os.path.join(BUCKET_DIR, nombre_objeto)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(contenido, f, ensure_ascii=False, indent=2)
    return nombre_objeto


# ------------------------- Lógica de la tarea -------------------------
def procesar_tarea(tarea):
    """
    Procesa una tarea y devuelve un resultado.
    Simula trabajo pesado con time.sleep para que se note la concurrencia.
    """
    time.sleep(1)  
    titulo = tarea.get("titulo", "sin título")
    descripcion = tarea.get("descripcion", "")

    return {
        "estado": "completado",
        "mensaje": f"Tarea '{titulo}' procesada. Descripción: '{descripcion}'",
        "worker_asignado": threading.current_thread().name
    }


# ------------------------- Worker (consume de la cola) -------------------------
def worker_loop(nombre_worker):
    """
    Cada hilo del pool ejecuta este bucle:
    toma un item (corr_id, conn, addr, tarea) de la cola,
    procesa la tarea, guarda el resultado y despierta al hilo que lo espera.
    """
    threading.current_thread().name = nombre_worker

    while not apagando.is_set():
        try:
            item = cola_tareas.get(timeout=1.0)
        except queue.Empty:
            continue

        if item is None:              
            cola_tareas.task_done()
            break

        corr_id, conn, addr, tarea = item
        nombre_hilo = threading.current_thread().name
        print(f"[{nombre_hilo}] Procesando tarea de {addr}...")

        try:

            resultado = procesar_tarea(tarea)

            nombre_objeto = f"tarea_{addr[0]}_{addr[1]}_{time.time_ns()}.json"
            guardar_en_bucket(nombre_objeto, resultado)
            resultado["objeto_s3"] = nombre_objeto

            with lock_resultados:
                pendiente = resultados_pendientes.get(corr_id)
                if pendiente:
                    pendiente["data"] = resultado
                    pendiente["evento"].set()

        except Exception as e:
            print(f"[{nombre_hilo}] Error con {addr}: {e}")
            with lock_resultados:
                pendiente = resultados_pendientes.get(corr_id)
                if pendiente:
                    pendiente["data"] = {"status": "error", "detalle": str(e)}
                    pendiente["evento"].set()
        finally:
            cola_tareas.task_done()


# ------------------------- Atención al cliente -------------------------
def manejar_cliente(conn, addr):
    """
    Se ejecuta en un hilo por cada cliente conectado.
    Lee la tarea (JSON), la encola y espera el resultado correspondiente.
    Es el ÚNICO dueño del socket: solo este hilo lo cierra.
    """
    print(f"[Servidor] Cliente conectado: {addr}")
    try:
        conn.settimeout(5.0)
        data = conn.recv(4096)
        if not data:
            return

        try:
            tarea = json.loads(data.decode())
        except json.JSONDecodeError:
            conn.sendall(json.dumps({"error": "JSON inválido"}).encode())
            return

        # Generamos un id único para correlacionar tarea <-> resultado
        corr_id = f"{addr[0]}:{addr[1]}:{time.time_ns()}"
        evento = threading.Event()
        with lock_resultados:
            resultados_pendientes[corr_id] = {"evento": evento, "data": None}

        # Encolamos la tarea YA leída (no el socket para que el worker lea)
        cola_tareas.put((corr_id, conn, addr, tarea))

        # Esperamos el resultado (máx 30s)
        if not evento.wait(timeout=30):
            with lock_resultados:
                resultados_pendientes.pop(corr_id, None)
            conn.sendall(json.dumps({"error": "Timeout esperando resultado"}).encode())
            return

        with lock_resultados:
            resultado = resultados_pendientes.pop(corr_id)["data"]

        conn.sendall(json.dumps(resultado).encode())

    except socket.timeout:
        print(f"[Servidor] Timeout con {addr}")
    except ConnectionResetError:
        print(f"[Servidor] Cliente {addr} desconectado abruptamente.")
    except Exception as e:
        print(f"[Servidor] Error con {addr}: {e}")
    finally:
        # Único lugar donde se cierra el socket del cliente
        try:
            conn.close()
        except Exception:
            pass
        print(f"[Servidor] Conexión cerrada con {addr}")


# ------------------------- Main -------------------------
def main():
    # 1) Levantamos el pool de hilos (los "workers")
    pool = ThreadPoolExecutor(max_workers=NUM_WORKERS)
    for i in range(NUM_WORKERS):
        pool.submit(worker_loop, f"Hilo {i + 1}")

    # 2) Levantamos el socket TCP
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(20)
    server.settimeout(1.0)
    print(f"[Servidor] Escuchando en {HOST}:{PORT} con {NUM_WORKERS} workers")

    try:
        while not apagando.is_set():
            try:
                conn, addr = server.accept()
                # Un hilo por cliente para leer la tarea y encolarla
                threading.Thread(
                    target=manejar_cliente, args=(conn, addr), daemon=True
                ).start()
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        print("\n[Servidor] Ctrl+C detectado, cerrando ordenadamente...")
    finally:
        # 3) Apagado ordenado
        apagando.set()

        # Señal de fin a los workers
        for _ in range(NUM_WORKERS):
            try:
                cola_tareas.put_nowait(None)
            except queue.Full:
                break

        # No esperamos a los hilos colgados; cancelamos los pendientes
        pool.shutdown(wait=False, cancel_futures=True)
        server.close()
        print("[Servidor] Cerrado.")


if __name__ == "__main__":
    main()