# PFO 3 — Rediseño como Sistema Distribuido (Cliente-Servidor)

Práctica Formativa Obligatoria 3 de la cátedra **Programación sobre Redes**.

Transformación de un sistema en una arquitectura distribuida usando **sockets TCP**,
con una **cola de mensajes**, un **pool de hilos** y **almacenamiento tipo objeto**.

---

## Integrantes

- Luis Barnabó

**Materia:** Programación sobre Redes
**Año:** 2026

---

## Arquitectura

El sistema sigue el patrón **productor-consumidor**:

1. El **cliente** se conecta por TCP y envía una tarea en formato JSON.
2. El **servidor** acepta la conexión, lee la tarea y la encola en una cola de mensajes.
3. Un **pool de hilos (workers)** consume las tareas de la cola y las procesa en paralelo.
4. Cada resultado se **persiste** en un almacenamiento tipo S3 (carpeta local).
5. El servidor devuelve el resultado al cliente por el mismo socket.

### Diagrama

```mermaid
flowchart TD
    subgraph Clientes["Clientes"]
        C1["Cliente móvil"]
        C2["Cliente web"]
        C3["Cliente de escritorio<br/>(cliente_tareas.py)"]
    end

    subgraph Balanceo["Balanceo de carga"]
        LB["Nginx / HAProxy<br/>(diseño, no implementado)"]
    end

    subgraph Servidor["Servidor de sockets (servidor_tareas.py)"]
        S["accept()<br/>0.0.0.0:5000"]
        H["Hilo por cliente<br/>manejar_cliente()"]
    end

    Q[["Cola de mensajes<br/>queue.Queue"]]

    subgraph Pool["Pool de hilos (ThreadPoolExecutor)"]
        W1["Hilo 1"]
        W2["Hilo 2"]
        W3["Hilo 3"]
        W4["Hilo 4"]
        W5["Hilo 5"]
    end

    R[("Resultados pendientes<br/>dict + threading.Event")]

    subgraph Almacenamiento["Almacenamiento"]
        S3[("Bucket S3 simulado<br/>carpeta resultados/<br/>1 JSON por tarea")]
        DB[("PostgreSQL<br/>diseño futuro")]
    end

    C1 -->|TCP| LB
    C2 -->|TCP| LB
    C3 -->|TCP| S
    LB -->|TCP| S

    S --> H
    H -->|put tarea| Q

    Q -->|get| W1
    Q -->|get| W2
    Q -->|get| W3
    Q -->|get| W4
    Q -->|get| W5

    W1 -->|set resultado| R
    W2 -->|set resultado| R
    W3 -->|set resultado| R
    W4 -->|set resultado| R
    W5 -->|set resultado| R

    W1 -->|persiste| S3
    W2 -->|persiste| S3
    W3 -->|persiste| S3
    W4 -->|persiste| S3
    W5 -->|persiste| S3

    R -->|respuesta| H
    H -->|TCP| C3
    H -.->|diseño futuro| DB

    classDef implementado fill:#d4edda,stroke:#28a745,color:#000
    classDef diseno fill:#fff3cd,stroke:#ffc107,color:#000,stroke-dasharray: 5 5

    class S,H,Q,W1,W2,W3,W4,W5,R,S3,C3 implementado
    class LB,C1,C2,DB diseno
```

### Leyenda

- **Verde**: componente implementado en el código.
- **Amarillo punteado**: componente incluido en el diseño arquitectónico

### Correspondencia con la consigna

| Ítem de la consigna | En el diagrama | Estado |
|---|---|---|
| Clientes (móviles, web) | `Clientes` | Representados (el cliente de escritorio es el implementado) |
| Balanceador de carga (Nginx/HAProxy) | `LB` | Diseño |
| Servidores workers con pool de hilos | `Pool de hilos` | Implementado |
| Cola de mensajes (RabbitMQ) | `queue.Queue` | Implementado (equivalente en memoria) |
| Almacenamiento distribuido (PostgreSQL, S3) | `S3` y `DB` | S3 simulado con carpeta local; PostgreSQL diseño futuro |