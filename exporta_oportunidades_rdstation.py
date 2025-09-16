import requests
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass
from time import sleep

@dataclass
class APIConfig:
    base_url: str
    token: str
    per_page: int = 200
    retry_attempts: int = 3
    retry_delay: int = 5

class RDStationClient:
    def __init__(self, config: APIConfig):
        self.config = config
        self._setup_logging()
        self.headers = {'accept': 'application/json'}

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('rd_station_sync.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def fetch_deals(self, date: datetime, page: int = 1) -> Optional[Dict]:
        start_datetime = date.replace(hour=0, minute=0, second=1)
        end_datetime = date.replace(hour=23, minute=59, second=59)

        params = {
            'token': self.config.token,
            'created_at_period': True,
            'start_date': start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            'end_date': end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            'page': page,
            'per_page': self.config.per_page
        }

        for attempt in range(self.config.retry_attempts):
            try:
                response = requests.get(
                    self.config.base_url,
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                self.logger.info(f"✅ Dados de {date.strftime('%d/%m/%Y')} (Página {page}) obtidos com sucesso.")
                return response.json()

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"❌ Tentativa {attempt + 1} falhou para {date.strftime('%d/%m/%Y')} (Página {page}): {str(e)}")
                if attempt < self.config.retry_attempts - 1:
                    self.logger.info(f"⏳ Aguardando {self.config.retry_delay}s para nova tentativa...")
                    sleep(self.config.retry_delay)
                else:
                    self.logger.error("❌ Falha definitiva após várias tentativas.")
                    return None

class DataExporter:
    def __init__(self, output_directory: str):
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_deals(self, data: Dict, date: datetime, page: int) -> Optional[Path]:
        deals = data.get("deals", [])
        if not deals:
            return None  # não salva se estiver vazio

        filename = self.output_dir / f"oportunidades_{date.strftime('%Y-%m-%d')}_p{page}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return filename

def main():
    config = APIConfig(
        base_url='https://crm.rdstation.com/api/v1/deals',
        token='66259070c34464001534835a'
    )

    output_dir = Path(r'G:\.shortcut-targets-by-id\1kArAZwgCxrjbQwQOPEzeJLtMUll3VVJ7\13. Dados\13.9. Atendimento\13.9.2. RD Station\13.9.2.1. Oportunidade_2026')
    
    client = RDStationClient(config)
    exporter = DataExporter(output_dir)

    start_date = datetime(2024, 7, 30)
    end_date = datetime.now()
    
    current_date = start_date
    while current_date <= end_date:
        page = 1
        while True:
            data = client.fetch_deals(current_date, page)
            if not data:
                break

            file_path = exporter.save_deals(data, current_date, page)
            if file_path:
                logging.info(f"✅ Página {page} de {current_date.strftime('%d/%m/%Y')} salva em {file_path}")
            else:
                logging.info(f"⚠️ Página {page} de {current_date.strftime('%d/%m/%Y')} estava vazia. Encerrando buscas para este dia.")
                break  # se os dados estão vazios, não precisa ir pra próxima página

            # Critério de parada mais seguro: menos que `per_page` registros
            if len(data.get("deals", [])) < config.per_page:
                logging.info(f"📋 Fim das páginas para o dia {current_date.strftime('%d/%m/%Y')}")
                break

            page += 1

        current_date += timedelta(days=1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Erro inesperado: {str(e)}")
