import os
from pathlib import Path

from flask import Flask, jsonify
from flask_cors import CORS

from routes.analysis import analysis_bp
from routes.chatbot import chatbot_bp
from routes.upload import upload_bp
from services.ml_model import AutoCategorizer
from services.openai_service import AzureOpenAIService
from services.storage import AzureBlobStorageAdapter, ExpenseRepository, LocalStorageAdapter


def create_app() -> Flask:
    app = Flask(__name__)

    base_dir = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("DATA_DIR", base_dir / "data"))
    db_path = data_dir / "expenses.db"
    upload_dir = data_dir / "uploads"

    # Parse config with safe defaults for local development.
    app.config["PDF_PARSER"] = os.getenv("PDF_PARSER", "pymupdf").lower().strip()
    app.config["STORE_UPLOADS"] = os.getenv("STORE_UPLOADS", "true").lower().strip() == "true"
    app.config["ANOMALY_CONTAMINATION"] = float(os.getenv("ANOMALY_CONTAMINATION", "0.08"))
    app.config["MAX_UPLOAD_SIZE_BYTES"] = int(os.getenv("MAX_UPLOAD_SIZE_MB", "10")) * 1024 * 1024
    app.config["AZURE_DOC_INTELLIGENCE_ENDPOINT"] = os.getenv("AZURE_DOC_INTELLIGENCE_ENDPOINT", "").strip()
    app.config["AZURE_DOC_INTELLIGENCE_KEY"] = os.getenv("AZURE_DOC_INTELLIGENCE_KEY", "").strip()

    cors_origin = os.getenv("CORS_ORIGIN", "*")
    CORS(app, resources={r"/*": {"origins": cors_origin}})

    # Initialize storage / ML helpers.
    repo = ExpenseRepository(db_path=db_path)
    categorizer = AutoCategorizer.default()
    az_storage_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    az_storage_container = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "").strip()
    file_storage = None
    if az_storage_conn and az_storage_container:
        try:
            file_storage = AzureBlobStorageAdapter(
                connection_string=az_storage_conn,
                container_name=az_storage_container,
            )
        except Exception:
            file_storage = None
    if file_storage is None:
        file_storage = LocalStorageAdapter(base_dir=upload_dir)

    app.extensions["repo"] = repo
    app.extensions["categorizer"] = categorizer
    app.extensions["file_storage"] = file_storage

    # Optional Azure OpenAI setup. If config is missing, the chatbot will fall back to heuristics.
    openai_service = None
    az_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
    az_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
    az_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip()
    az_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview").strip()
    az_timeout = float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "20"))

    if az_key and az_endpoint and az_deployment:
        try:
            openai_service = AzureOpenAIService(
                api_key=az_key,
                endpoint=az_endpoint,
                deployment=az_deployment,
                api_version=az_api_version,
                timeout_seconds=az_timeout,
            )
        except Exception:
            openai_service = None
    app.extensions["openai_service"] = openai_service

    # Register endpoints.
    app.register_blueprint(upload_bp, url_prefix="/upload")
    app.register_blueprint(analysis_bp)
    app.register_blueprint(chatbot_bp)

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "expenseCount": repo.count()})

    @app.errorhandler(404)
    def not_found(_e):
        return jsonify({"error": "Not found"}), 404

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

