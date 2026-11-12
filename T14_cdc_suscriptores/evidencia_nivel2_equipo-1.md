# Evidencia del Nivel 2 · equipo-1

| Medida | Valor |
|---|---|
| Perfil: suscriptores · cambios · prob_baja_dura | 105 · 32 · 0.343 |
| Altas · modificaciones · bajas generadas | 8 · 13 · 11 |
| Altas · modificaciones · bajas capturadas | 8 · 13 · 0 |
| maestro_suscriptores inicial · tras el flujo | 105 · 102 |
| stg_cambios_suscriptor tras la corrida 1 · tras la corrida 2 | 105 · 126 |
| Lo que cree el destino · origen real · fantasmas | 113 · 102 · 11 |
| Checkpoint final | 2024-06-02 09:00:00 |

## Generadas frente a capturadas (corrida 2)

 tipo_cambio  generadas  capturadas  no_capturadas
        alta          8           8              0
modificacion         13          13              0
        baja         11           0             11

## Repeticion con el checkpoint restablecido

| Medida | Valor |
|---|---|
| Checkpoint restablecido a | 2024-06-01 06:00:00 |
| Filas duplicadas en stg_cambios_suscriptor | 21 |
| Filas sobrantes por pareja suscriptor-tipo | 21 |
| Altas + modificaciones del perfil | 21 |
| Checkpoint final | 2024-06-02 09:00:00 |
