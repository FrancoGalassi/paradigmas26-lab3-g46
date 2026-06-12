# Grupo 46 - Integrantes
 - Bosque, Lissandro
 - Galassi, Franco
 - Pairetti, Joaquín

## Ejercicio 1
a)
![Diagrama](./Informe-media/diagrama.png)

b)


| Paso | Abstracción Spark |
|------|------------------|
| Lectura de subscriptions | Ninguna (driver) | 
| Conexión y descarga del feed | `flatMap` |
| Filtrar posts | `filter` |
| Cargar diccionario | Ninguna (driver) |
| Extraer entidades nombradas | `flatMap` |
| Clasificar entidades | `map` |
| Contar entidades | `reduceByKey` |
| Imprimir estadísticas | Ninguna (driver) |


**Lectura de subscriptions**

No encaja en ninguna abstracción de Spark porque ocurre antes de que exista un RDD. Es una operación de inicialización que el driver necesita hacer primero para saber qué URLs paralelizar. No hay elementos sobre los cuales mapear todavía.


**Cargar diccionario**
Tampoco encaja porque es un input auxiliar que todos los workers necesitan compartir, no algo que se procesa en paralelo. 


**Imprimir estadísticas** 

Imprimir es un efecto secundario, no una transformación de datos. Spark requiere que las funciones que se pasan a map, flatMap etc. sean puras (sin efectos secundarios) para poder distribuirlas y reejecutarlas si fallan. Además necesita todos los datos ya reducidos y ordenados antes de imprimir, lo cual solo el driver puede hacer.


c)
En el esqueleto incial no existe ninguna barrera porque no hay paralelismo, al momento de paralelizar con Spark, si las hay.

Barreras en pipeline
---

**Contar entidades**
(usando `reduceByKey`) es la única barrera real. Para saber cuántas veces apareció "Python" en total, hay que esperar que todos los workers terminen de extraer entidades de todos sus posts. Ningún worker puede producir el conteo final hasta que todos hayan terminado.

**Imprimir** las estadísticas también podría llegar a ser una barrera, porque el driver no puede imprimir el ranking hasta tener todos los conteos.

Independientes en pipeline
---
**Descarga y parseo** cada worker descarga su URL sin saber nada de los otros.

**Filtrado de posts** cada worker filtra sus propios posts de forma independiente.

**Extracción de entidades** cada worker procesa sus propios posts sin coordinarse.

**Clasificación (map a pares ((tipo, nombre), 1))** cada entidad se transforma de forma completamente independiente.


d)

Las funciones que se pasan a las transformaciones de Spark deben cumplir tres restricciones para poder ejecutarse en un entorno distribuido.

**Serialización:** Cuando Spark distribuye una transformación, serializa la función y todo el contexto que esta captura para enviarlo por la red a cada worker. Por esta razón, todos los objetos que la función referencie deben implementar la interfaz `Serializable`. Si algún objeto capturado no es serializable, Spark lanza una excepción antes de ejecutar cualquier tarea.
Por ejemplo, las entidades en `Dictionary` deberán ser serializadas para su correcto funcionamiento.


**Estado compartido:** Las funciones no pueden leer ni escribir estado compartido mutable. En un cluster distribuido, cada worker ejecuta en una máquina con su propia memoria independiente, no existe memoria compartida entre workers ni entre workers y el driver. Si una función modifica una variable del driver desde dentro de una transformación, cada worker opera sobre su propia copia local y los cambios nunca se propagan. El único mecanismo provisto por Spark para que los workers comuniquen información al driver de forma segura son los Accumulators, que solo permiten operaciones de incremento.

**Efectos secundarios:** Las funciones deben ser puras o carecer de efectos secundarios observables. Spark puede reejecutar una tarea si esta falla, lo que implica que una misma función puede invocarse múltiples veces sobre el mismo elemento. Si la función tiene efectos secundarios, estas rejecuciones pueden producir resultados incorrectos o duplicados. En modo local los `println` dentro de transformaciones son visibles porque driver y workers comparten el mismo proceso, pero en un cluster real los logs de los workers solo son accesibles a través de la Spark UI.
## Ejercicio 2
**¿qué pasaría si dejaran propagar la excepción?**

Spark reintentaría la tarea múltiples veces hasta llegar a su límite (por defecto 4 intentos) y a partir de ahí la consideraría fallida y abortaría todo el proceso.

## Ejercicio 3

**`reduceByKey` es una barrera de sincronización. ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable para este problema?**

Cuando Spark ejecuta un `reduceByKey` todos los workers reorganizan sus datos de forma que todos los pares con la misma clave queden en el mismo worker. Una vez redistribuidos, cada worker aplica la función de reducción sobre los pares que le tocaron. Ningún worker puede producir su resultado parcial hasta haber recibido todos los datos que le corresponden, y el driver no puede continuar hasta que todos los workers terminen. 
Es inevitable para este problema porque para saber cuántas veces apareció "Python" en total es imposible saberlo sin combinar los conteos parciales de todos los workers. No existe forma de calcular este resultado de manera completamente independiente por cada worker.

**¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`?**

La función debe ser **asociativa** y **conmutativa**. Spark realiza reducciones parciales en cada worker antes de enviar datos por la red, para minimizar la transferencia. Esto significa que los valores se combinan en orden y agrupaciones no determinísticas dependiendo de cómo se distribuya la carga. 

**¿Dónde se hace la lectura del diccionario?**

La lectura del diccionario se hace en el **driver**, antes de cualquier transformación distribuida. El diccionario se carga con `Dictionary.loadAll(cmdArgs.entitiesDir)` y el resultado es una `List[NamedEntity]` que luego se captura en el closure del `flatMap`. Spark serializa esa lista y la envía a cada worker junto con la función. 

