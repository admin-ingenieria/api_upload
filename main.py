from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Importar los routers (planificacion y recoleccion)
from planificacion.routes import reco_router as planificacion_router
from recoleccion.routes import reco_router as recoleccion_router

# Crear instancia
app = FastAPI()

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(planificacion_router, prefix="/planificacion")
app.include_router(recoleccion_router, prefix="/recoleccion")
