import socket
import json

HOST = '127.0.0.1'   
PORT = 5000


def enviar_tarea():
    print("\n--- ENVIAR NUEVA TAREA ---")
    print("(Escribe 'salir' para cancelar)")

    titulo = input("Título de la tarea: ").strip()
    if titulo.lower() == 'salir':
        return

    descripcion = input("Descripción: ").strip()

    # Empaquetamos los datos
    tarea = {
        "titulo": titulo,
        "descripcion": descripcion
    }

    # Serializamos a string JSON y luego a bytes
    datos_enviar = json.dumps(tarea).encode('utf-8')

    try:
        # Usamos el bloque 'with' para asegurar que el socket se cierre automáticamente
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cliente:
            # Nos conectamos al servidor
            cliente.connect((HOST, PORT))
            print("\n[CLIENTE] Conectado al servidor. Enviando tarea...")

            # Enviamos los datos
            cliente.sendall(datos_enviar)

            # Esperamos la respuesta (se bloquea hasta que un worker termine)
            print("[CLIENTE] Esperando a que un worker procese la tarea...")
            datos_recibidos = cliente.recv(4096).decode('utf-8')

            # Verificamos que el servidor haya respondido algo
            if not datos_recibidos:
                print("[ERROR] El servidor cerró la conexión sin responder.")
                return

            # Procesamos el resultado
            respuesta = json.loads(datos_recibidos)

            print("\n================ RESULTADO ===============")
            print(f"Estado: {respuesta.get('estado').upper()}")
            print(f"Mensaje: {respuesta.get('mensaje')}")
            print(f"Atendido por: {respuesta.get('worker_asignado')}")
            print(f"Guardado en: {respuesta.get('objeto_s3')}")
            print("==========================================\n")

    except ConnectionRefusedError:
        print("[ERROR] No se pudo conectar. ¿Está encendido 'servidor_tareas.py'?")
    except Exception as e:
        print(f"[ERROR INESPERADO] {e}")


def menu_principal():
    while True:
        print("\n==================================")
        print(" CLIENTE DE TAREAS (DISTRIBUIDO)")
        print("==================================")
        print("1. Enviar tarea a la red")
        print("2. Salir")
        opcion = input("Seleccione una opción (1-2): ").strip()

        if opcion == '1':
            enviar_tarea()
        elif opcion == '2':
            print("Saliendo del cliente. ¡Hasta luego!")
            break
        else:
            print("Opción no válida. Intente nuevamente.")


if __name__ == "__main__":
    menu_principal()