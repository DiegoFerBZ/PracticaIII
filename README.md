# Plan: Fases concurrentes y Streaming en los procesos de Contabilidad


| | Hoy | Propuesto |
|---|---|---|
| Armado de transacciones | Se arma **todo** en memoria antes de enviar nada | Se arma por lotes/cuentas y se envía **a medida que está listo** |
| Consulta de información de cuenta y saldos | 1 llamada HTTP por cuenta; con un volumen alto de cuentas se hacen muchas llamadas | **1 llamada por lote** (500 por defecto), varios lotes en paralelo |
| Envío al Core | 1 POST con la lista completa | 1 POST **en streaming** (un elemento por flush) |
| Consecutivos | 1 por transacción, pedidos uno a uno dentro del Core | **1 llamada por ejecución** reserva el rango completo |
| Persistencia en el Core | Bloques secuenciales de tamaño fijo (todo o nada por bloque) | **N workers** concurrentes, 1 transacción de BD **por transacción contable** |
| Memoria | Crece con el volumen del job | Acotada por el tamaño de los buffers (backpressure) |
| Fallo de una cuenta | Puede abortar el job completo | Se aísla: se omite esa cuenta y el resto continúa |

---

## 1. Analizar el flujo actual e identificar puntos de procesamiento secuencial

### 1.1 Flujo actual (línea base)

``` mermaid
flowchart TD
    A["Invocador"] --> B["Contabilidad<br/>valida auditoría del job"]
    B --> C["Lee parámetros del día<br/>y eventos de origen"]
    C --> D["Agrupa por cuenta<br/>y obtiene datos de cuenta"]
    D --> E["Por cuenta o evento:<br/>consulta de información de cuenta<br/>1 a 1 por HTTP"]
    E --> F["Por cuenta: consulta de saldo<br/>1 a 1 o 1 global sin límite"]
    F --> G["Arma TODAS las transacciones<br/>en memoria"]
    G --> H["POST único no streaming<br/>con la lista completa"]
    H --> I["Core: por cada ítem<br/>pide consecutivo uno a uno"]
    I --> J["Core: confirma por bloques<br/>de tamaño fijo, secuencial"]
    J --> K["Actualiza auditoría del job<br/>responde 200"]

    classDef seq fill:#fde2e2,stroke:#c0392b,color:#000
    class E,F,G,H,I,J seq
```

Los nodos en rojo son los puntos de procesamiento secuencial o de barrera (detalle en 1.2).

### 1.2 Puntos de procesamiento secuencial identificados

| # | Punto | Dónde ocurre | Por qué frena el proceso |
|---|---|---|---|
| S1 | **Consulta de información de cuenta 1 a 1** | Contabilidad → Core, una llamada por cuenta o incluso por evento | Con miles de cuentas son miles de *round-trips* HTTP; es el cuello de botella principal. En algunos procesos se repite la consulta para una misma cuenta |
| S2 | **Barrera de armado en memoria** | Ciclos secuenciales (en algunos procesos se recorren todas las cuentas una vez por cada componente de la transacción) o un semáforo de concurrencia fija seguido de una espera global | No se envía nada hasta que **todas** las cuentas terminan; el consumo de memoria crece con el volumen |
| S3 | **Envío único no streaming** | Lista completa en un solo cuerpo de solicitud | El Core no empieza a persistir hasta recibir y deserializar todo el cuerpo |
| S4 | **Consecutivo por ítem** | Core, dentro del ciclo de persistencia | Una consulta/actualización por transacción sobre un contador compartido |
| S5 | **Persistencia por bloques secuenciales** | Core, bloques de tamaño fijo | Un solo hilo y una sola conexión; si falla un ítem se revierte el bloque entero |
| S6 | **Aborto total ante un fallo individual** | Una cuenta con información o saldo faltante lanza excepción que se propaga a todo el job | Una cuenta con datos incompletos tumba el job completo |


### 1.3 Medición de tiempos de procesamiento

**Dato base disponible:** 9 ejecuciones consecutivas del Proceso A, agosto 2026.

| Fecha de ejecución | Duración (min) |
|---|---:|
| 12/08/2026 | 29 |
| 13/08/2026 | 28 |
| 14/08/2026 | 29 |
| 15/08/2026 | 30 |
| 16/08/2026 | 30 |
| 17/08/2026 | 31 |
| 18/08/2026 | 31 |
| 19/08/2026 | 31 |
| 20/08/2026 | 33 |

| Estadístico | Valor |
|---|---:|
| Ejecuciones | 9 |
| Mínimo / Máximo | 28 / 33 min |
| Promedio | 30,2 min |
| Tiempo total acumulado | 272 min (≈ 4,5 h) |
| Crecimiento 13/08 → 20/08 | +5 min (+18 %) en 7 días |

**Lectura:**

- El proceso toma **~30 minutos** de forma consistente: la variación es baja (rango de 5 min), lo que indica un costo dominado por trabajo repetitivo y no por picos aislados.
- La **tendencia es creciente** (de 28 a 33 min en una semana, 7 de las 8 variaciones diarias son ≥ 0).

## 2. Diseño general

### 2.1 Principios

1. **Fases, no pasos.** Cada trabajo (consultar, enriquecer, armar, enviar) es una fase independiente con su propio grado de paralelismo.
2. **Streaming de extremo a extremo.** Un resultado sale de una fase hacia la siguiente en cuanto está listo, sin esperar a los demás.
3. **Backpressure obligatorio.** Todo canal entre fases es acotado: si el consumidor es más lento, el productor espera. Nunca se acumula un volumen arbitrario en memoria.
4. **I/O por lote, CPU por elemento.** Lo que cruza la red se agrupa en lotes (una llamada por 500 cuentas); lo que es cómputo en memoria se reparte por cuenta.
5. **Aislamiento de fallos por elemento.** Un elemento defectuoso se omite y se registra; el resto continúa.
6. **Falla rápida cuando es sistémico.** Si cae el consumidor (o falla una fase sin manejador de error), se cancela todo el pipeline de inmediato.
7. **Coexistencia.** Los servicios legados del Core no se modifican: los de streaming son nuevos y conviven con ellos.

### 2.2 Vista de conjunto

``` mermaid
flowchart LR
    subgraph CONT["Contabilidad  PRODUCTOR"]
        direction LR
        O["Origen<br/>lotes o cuentas"] --> F1["Fase 1<br/>Enriquecer por lote<br/>información de cuenta / saldos<br/>I/O"]
        F1 --> F2["Fase 2<br/>Armar transacción<br/>por cuenta<br/>CPU"]
        F2 --> C["Productor final<br/>flujo asíncrono"]
    end

    C == "POST stream JSON<br/>un flush por elemento" ==> P

    subgraph CORE["Core  CONSUMIDOR"]
        direction LR
        P["Consumidor único<br/>lee el stream"] --> CH["Canal acotado<br/>workers x 2"]
        CH --> W1["Worker 1<br/>1 conexión"]
        CH --> W2["Worker 2<br/>1 conexión"]
        CH --> WN["Worker N<br/>1 conexión"]
    end

    W1 --> DB[("DB<br/>movimientos y saldos")]
    W2 --> DB
    WN --> DB

    classDef io fill:#e8f1fb,stroke:#2e6da4,color:#000
    classDef cpu fill:#eaf7ea,stroke:#2e8b57,color:#000
    class F1 io
    class F2 cpu
```

Los canales entre fases (acotados, con espera cuando están llenos) son los que transmiten la presión hacia atrás: si los workers del Core se saturan, el stream HTTP se frena, el consumidor final deja de leer, y cada fase anterior queda bloqueada en su escritura hasta que haya espacio.

### 2.3 Componentes genéricos (reutilizables por cualquier proceso)

| Componente | Responsabilidad | Servicio |
|---|---|---|
| Constructor de pipeline (origen) | Punto de entrada. Vuelca el origen a un canal acotado (1.000 por defecto) | Contabilidad |
| Fase 1→1 | Ejecuta la lógica por elemento con *N* workers y publica cada resultado de inmediato | Contabilidad |
| Fase 1→N (o 1→0) | Por ejemplo un lote que se expande a una cuenta por elemento, o una cuenta omitida que no emite nada | Contabilidad |
| Cierre por stream | Entrega todo como un único flujo asíncrono para el POST en streaming | Contabilidad |
| Creacion de lotes | Parte una colección en lotes del tamaño configurado | Contabilidad |
| Proveedor de consecutivos por lote | Reserva en **una** llamada el rango de consecutivos de la ejecución y los reparte en memoria | Contabilidad |
| Emisor de stream | Envía el stream, cuenta lo transmitido, valida la respuesta y registra trazabilidad | Contabilidad |
| Contenido HTTP en streaming | Escribe un array JSON con *flush* tras cada elemento | Contabilidad |
| Procesador concurrente por worker | Consumidor concurrente: bomba + *N* workers, 1 unidad de trabajo (conexión) por worker | Core |


### 2.4 Variantes de flujo

Todos los procesos son una combinación de las mismas piezas. Se distinguen tres formas según qué datos externos necesitan por cuenta:

``` mermaid
flowchart LR
    subgraph VA["Variante A  sin consulta externa"]
        direction LR
        A0["Cuentas"] --> A2["Armar<br/>por cuenta"] --> AS["Stream"]
    end
    subgraph VB["Variante B  información de cuenta por lote"]
        direction LR
        B0["Lotes de cuentas"] --> B1["Información de cuenta<br/>por lote, 1 a N"] --> B2["Armar<br/>por cuenta"] --> BS["Stream"]
    end
    subgraph VC["Variante C  saldos e información de cuenta por lote"]
        direction LR
        C0["Lotes de cuentas"] --> C1["Saldos por lote<br/>filtra suficientes"] --> C2["Información de cuenta<br/>por lote, 1 a N"] --> C3["Armar<br/>por cuenta"] --> CS["Stream"]
    end
```

---

## 3. Diseño de productores de datos

El **productor** es el microservicio Contabilidad: lee las fuentes, enriquece, arma las transacciones contables y las emite como stream.

### 3.1 Preparación previa al pipeline (secuencial, una sola vez)

Lo que no se puede paralelizar o es barato se hace una vez antes de arrancar.

### 3.2 Origen y partición en lotes

- Si la fase siguiente hace I/O (saldos, información de cuenta): el origen son **lotes** (500 por defecto).
- Si no hay I/O: el origen son las **cuentas**.
- El canal de entrada es acotado (1.000 por defecto), de modo que enumerar un origen grande no bufferiza todo en memoria.

### 3.3 Fases productoras

| Tipo de fase | Naturaleza | Paralelismo | Qué hace |
|---|---|---|---|
| Enriquecimiento por lote (saldos) | I/O de red | Bajo | Una llamada por lote; descarta cuentas sin saldo suficiente y acumula las omitidas |
| Enriquecimiento por lote (información de cuenta) | I/O de red | Bajo  | Una llamada por lote; expande 1→N cuentas con su información ya resuelta |
| Armado de la transacción | CPU, sin I/O | Medio  | Construye la transacción de la cuenta con el paquete completo de parámetros; asigna el siguiente consecutivo del lote |

---

## 4. Diseño de consumidores de datos

Hay **dos consumidores encadenados**: el consumidor final del pipeline de Contabilidad (que a su vez es un *cliente* HTTP en streaming) y el consumidor concurrente del Core.

### 4.1 Consumidor en Contabilidad: cliente de streaming

- Recibe el flujo asíncrono que cierra el pipeline y lo envía con **un único POST**.
- Usa un contenido HTTP propio que escribe un array JSON y hace **flush después de cada elemento**. Es deliberado: el serializador estándar acumula ~15 KB antes de escribir al socket, lo que con un productor lento retrasaría todo hasta el final y anularía el streaming.
- Cliente HTTP dedicado con **timeout propio de larga duración**, separado del cliente por defecto de timeout corto.
- Envía una cabecera con el identificador del job para correlacionarlo.
- Cuenta los elementos transmitidos para registrar el total real y valida la respuesta.

### 4.2 Consumidor en el Core: procesamiento concurrente por worker

``` mermaid
flowchart TD
    R["Endpoint del proceso<br/>lee el cuerpo como stream"] --> S["Servicio del proceso<br/>procesador concurrente"]
    S --> BM["Consumidor único<br/>escribe en canal acotado"]
    BM --> CH["Canal acotado<br/>capacidad workers x 2"]
    CH --> W["N workers de vida larga"]

    subgraph WK["Cada worker"]
        direction TB
        U["Crea 1 conexión al iniciar<br/>la reutiliza en todos sus ítems"] --> L["Lee un ítem del canal"]
        L --> PR["Preparar fuera de transacción<br/>consecutivo: usa el recibido o pide uno"]
        PR --> TX["Empezar transacción"]
        TX --> AC["Acción del proceso<br/>registra movimiento y actualiza saldos"]
        AC --> OK{"Éxito?"}
        OK -- "sí" --> CM["Guardar cambios"]
        OK -- "no" --> AB["Abortar solo este ítem<br/>registrar como fallido"]
        CM --> L
        AB --> L
    end

    W --> U
    W --> FIN["Al terminar todos:<br/>si hay fallidos lanza error de negocio<br/>si no responde éxito"]
```


---

## 5. Diseño del flujo de datos entre microservicios

### 5.1 Mapa de servicios

``` mermaid
flowchart LR
    INV["Invocador"] -- "dispara el proceso" --> CONT["Contabilidad"]
    CONT -- "SQL directo<br/>parámetros, eventos,<br/>auditoría del job" --> DBC[("BD de Contabilidad")]
    CONT -- "HTTP REST<br/>consultas por lote<br/>y consecutivos" --> CORE["Core"]
    CONT == "HTTP streaming<br/>transacciones" ==> CORE
    CORE -- "SQL<br/>movimientos, saldos,<br/>auditoría de eventos" --> DBK[("BD del Core")]
    CONT -- "logs y<br/>notificaciones" --> LOGS["Servicio de trazabilidad"]
```


---

## 6. Pasos a seguir

- Implementacion del diseño
- Evaluacion de resultados
