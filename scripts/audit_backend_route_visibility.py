from pathlib import Path
import json

APP = Path("mlb_app/app.py")
ROUTES = Path("mlb_app/model_projection_routes.py")
OUT = Path("tmp/backend_route_visibility_audit.json")
OUT.parent.mkdir(exist_ok=True)

app_text = APP.read_text()
routes_text = ROUTES.read_text()

checks = {
    "model_projection_router_imported": "model_projection_router" in app_text,
    "model_projection_router_included": "include_router(model_projection_router)" in app_text,
    "models_projections_route_defined": '@router.get("/models/projections")' in routes_text,
    "app_factory_present": "def create_app" in app_text,
    "app_object_created": "app = create_app()" in app_text,
}

payload = {
    "diagnosis": "backend_route_visibility_expected_present" if all(checks.values()) else "backend_route_visibility_missing",
    **checks,
    "recommended_runtime_check": "/debug/routes",
    "production_default_unchanged": True,
}

OUT.write_text(json.dumps(payload, indent=2))
print(json.dumps(payload, indent=2))
