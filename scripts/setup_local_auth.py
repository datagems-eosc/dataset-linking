"""Configure and verify DataGEMS OAuth credentials for local development."""

import getpass
import os
import stat
import sys
from pathlib import Path

from dotenv import set_key

from dl.datagems_auth import (
    DataGEMSAuthenticationError,
    get_access_token,
    reset_access_token_cache,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


def main() -> int:
    print("Dataset Linking - configurazione accesso DataGEMS DEV")
    print()
    print(
        "Il client secret deve essere fornito dal team responsabile "
        "tramite il canale autorizzato (Vault/password manager)."
    )
    print("Non verrà visualizzato né salvato nel repository.")
    print()

    client_id = input("Client ID [dataset-linking]: ").strip() or "dataset-linking"
    client_secret = getpass.getpass("Client secret: ").strip()
    if not client_secret:
        print("Errore: il client secret non può essere vuoto.", file=sys.stderr)
        return 1

    os.environ["OIDC_CLIENT_ID"] = client_id
    os.environ["OIDC_CLIENT_SECRET"] = client_secret
    reset_access_token_cache()

    print("Verifico le credenziali...")
    try:
        get_access_token(force_refresh=True)
    except DataGEMSAuthenticationError as exc:
        print(f"Verifica fallita: {exc}", file=sys.stderr)
        print("Il file .env non è stato modificato.", file=sys.stderr)
        return 1

    ENV_FILE.touch(exist_ok=True)
    if os.name != "nt":
        ENV_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)
    set_key(str(ENV_FILE), "OIDC_CLIENT_ID", client_id)
    set_key(str(ENV_FILE), "OIDC_CLIENT_SECRET", client_secret)

    print(f"Accesso verificato. Configurazione salvata in: {ENV_FILE}")
    print("Ora avvia Flask o FastAPI normalmente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
