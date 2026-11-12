"""API REST simulada de estado de facturacion de medidores.

Simulador didactico de un proveedor externo: proceso Python local (FastAPI +
uvicorn), no un contenedor. Se arranca y se detiene junto con la practica y no
agrega ningun servicio a docker-compose.yml.

Contrato:
  GET /salud                 -> sin autenticacion, confirma que el proceso esta arriba.
  GET /medidores/estado?page=N&page_size=M
      Header Authorization: Bearer <token>
      200 -> {"pagina", "page_size", "total_registros_informativo", "datos", "hay_siguiente"}
      401 -> token ausente o incorrecto (NO reintentable).
      404 -> pagina fuera de rango (NO reintentable).
      429 -> limite de tasa excedido (reintentable; puede traer "Retry-After").
      5xx -> error transitorio del proveedor (reintentable).

El cliente NUNCA debe calcular de antemano cuantas paginas hay: debe seguir
"hay_siguiente" hasta que sea falso. Una API real puede cambiar el numero de
paginas sin aviso: el campo "total_registros_informativo" se entrega solo como
referencia, no como contrato de paginacion.

Inyeccion de fallos y latencia, reproducible por equipo y por (pagina,
intento): ver generar_estado_medidores.parametros_equipo. El simulador cuenta
en memoria cuantas veces se ha pedido cada pagina desde que arranco el
proceso, y el resultado de cada intento depende solo de (equipo, pagina,
numero de intento): asi una corrida se puede repetir y dar los mismos codigos.
Por eso una segunda corrida contra el mismo proceso continua la cuenta de
intentos y no ve los mismos fallos que la primera. Un proveedor real no se
comporta asi; la simulacion lo hace para ser reproducible.

Sin --equipo (el ejemplo resuelto del Nivel 1), la pagina 3 responde SIEMPRE
503 en su primer intento, para que el ejemplo muestre un reintento; el resto
de paginas y todos los reintentos siguen siendo probabilisticos (con semilla).
Con --equipo, todo es probabilistico, sin falla garantizada.

Uso:
    python iniciar_api_simulada.py                          # ejemplo resuelto, sin --equipo
    python iniciar_api_simulada.py --equipo equipo-3 --puerto 8013
    python iniciar_api_simulada.py --acelerar                # Nivel 1 guiado: t0/t_max en fracciones de segundo

Puerto por defecto configurable con la variable de entorno API_PUERTO (util
si el puerto 8013 ya esta ocupado en su equipo).
"""
import argparse
import os
import time
import zlib

import numpy as np
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query

import generar_estado_medidores as gem

# Token exclusivamente didactico. En un despliegue real el token vive en un
# gestor de secretos, nunca en el codigo fuente ni en un valor por defecto.
TOKEN_ESPERADO = os.environ.get("API_TOKEN", "acueducto-token-didactico")

# Tope de latencia con --acelerar: reduce la latencia servida a una fraccion
# de segundo para que el Nivel 1 quepa en el tiempo de clase. Sin --acelerar
# se sirve la latencia completa del perfil.
TOPE_LATENCIA_ACELERADA_MS = 40.0


def crear_app(equipo=None, acelerar=False, forzar_fallo_pagina3=True, seed_base=gem.SEED):
    """Fabrica de la aplicacion FastAPI. Separada de main() para poder
    instanciarla en memoria (por ejemplo con TestClient, en pruebas) sin abrir
    un puerto real."""
    df, params = gem.generar_estado(equipo=equipo, seed_base=seed_base)
    total = len(df)
    etiqueta_semilla = equipo or "sin-equipo"

    app = FastAPI(title="API simulada de estado de facturacion")
    app.state.intentos_por_pagina = {}
    app.state.params = params
    app.state.total = total

    def _clasificar_intento(pagina, intento):
        """Devuelve el codigo HTTP que le corresponde a este intento, de forma
        determinista dado (equipo, pagina, intento): reproducible sin
        depender de temporizacion real de red."""
        if forzar_fallo_pagina3 and equipo is None and pagina == 3 and intento == 1:
            return 503
        seed_seq = np.random.SeedSequence([seed_base, zlib.crc32(etiqueta_semilla.encode()), pagina, intento])
        rng = np.random.default_rng(seed_seq)
        r = rng.random()
        if r < params["prob_429"]:
            return 429
        if r < params["prob_429"] + params["prob_5xx"]:
            return 503
        return 200

    def _latencia_s(pagina, intento):
        seed_seq = np.random.SeedSequence([seed_base, zlib.crc32(etiqueta_semilla.encode()), pagina, intento, 999])
        rng = np.random.default_rng(seed_seq)
        latencia_ms = params["latencia_base_ms"] + rng.uniform(0, params["jitter_ms"])
        if acelerar:
            latencia_ms = min(latencia_ms, TOPE_LATENCIA_ACELERADA_MS)
        return latencia_ms / 1000.0

    @app.get("/salud")
    def salud():
        return {"estado": "arriba", "equipo": etiqueta_semilla, "total_registros": total,
                "page_size_defecto": params["page_size"], "acelerado": acelerar}

    @app.get("/medidores/estado")
    def obtener_estado(page: int = Query(1, ge=1), page_size: int = Query(None, ge=1),
                        authorization: str = Header(None)):
        if authorization != f"Bearer {TOKEN_ESPERADO}":
            raise HTTPException(status_code=401, detail="Token invalido o ausente")

        ps = page_size or params["page_size"]
        intentos = app.state.intentos_por_pagina
        intento = intentos.get(page, 0) + 1
        intentos[page] = intento

        time.sleep(_latencia_s(page, intento))

        codigo = _clasificar_intento(page, intento)
        if codigo == 429:
            espera = "1" if acelerar else "2"
            raise HTTPException(status_code=429, detail="Limite de tasa excedido",
                                 headers={"Retry-After": espera})
        if codigo == 503:
            raise HTTPException(status_code=503, detail="Error transitorio del proveedor de facturacion")

        inicio = (page - 1) * ps
        fin = inicio + ps
        if inicio >= total:
            raise HTTPException(status_code=404, detail=f"Pagina {page} fuera de rango (total {total} registros)")

        datos = df.iloc[inicio:fin].to_dict(orient="records")
        hay_siguiente = fin < total
        return {"pagina": page, "page_size": ps, "total_registros_informativo": total,
                "datos": datos, "hay_siguiente": hay_siguiente}

    return app


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", choices=gem.EQUIPOS_VALIDOS, default=None)
    ap.add_argument("--puerto", type=int, default=int(os.environ.get("API_PUERTO", 8013)))
    ap.add_argument("--acelerar", action="store_true",
                     help="Reduce la latencia servida a fracciones de segundo (uso del Nivel 1 guiado)")
    ap.add_argument("--sin-falla-garantizada", action="store_true",
                     help="Desactiva la falla forzada de la pagina 3 del ejemplo sin --equipo (solo diagnostico)")
    a = ap.parse_args()

    app = crear_app(equipo=a.equipo, acelerar=a.acelerar, forzar_fallo_pagina3=not a.sin_falla_garantizada)
    print(f"API simulada -> http://127.0.0.1:{a.puerto} (equipo={a.equipo or 'sin-equipo'}, "
          f"acelerar={a.acelerar})")
    uvicorn.run(app, host="127.0.0.1", port=a.puerto, log_level="warning")


if __name__ == "__main__":
    main()
