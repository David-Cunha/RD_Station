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
    """Configuração para API do RD Station"""
    base_url: str
    token: str
    per_page: int = 200
    retry_attempts: int = 3
    retry_delay: int = 5


class RDStationClient:
    """Cliente para interação com a API do RD Station"""

    def __init__(self, config: APIConfig):
        self.config = config
        self._setup_logging()
        self.headers = {'accept': 'application/json'}

    def _setup_logging(self):
        """Configuração do sistema de logs"""
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
        """Busca oportunidades do RD Station para uma data e página específicas"""
        start_datetime = date.replace(hour=0, minute=0, second=1)
        end_datetime = date.replace(hour=23, minute=59, second=59)

        params = {
            'token': self.config.token,
            'created_at_period': 'custom',  # Corrigido
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
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()

                qtd = len(data) if isinstance(data, list) else len(data.get('deals', []))
                self.logger.info(f"✅ {date.strftime('%d/%m/%Y')} (Página {page}) - {qtd} registros")
                return data

            except requests.exceptions.RequestException as e:
                self.logger.error(f"❌ Tentativa {attempt + 1} falhou ({date.strftime('%d/%m/%Y')} p{page}): {e}")
                if attempt < self.config.retry_attempts - 1:
                    self.logger.info(f"⏳ Aguardando {self.config.retry_delay} segundos para nova tentativa...")
                    sleep(self.config.retry_delay)
                else:
                    self.logger.error(f"❌ Falha após {self.config.retry_attempts} tentativas")
                    return None


class DataExporter:
    """Gerencia a exportação dos dados para arquivos"""

    def __init__(self, output_directory: str):
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_deals(self, data: Dict, date: datetime, page: int) -> Path:
        """Salva as oportunidades em arquivo JSON"""
        filename = self.output_dir / f"oportunidades_{date.strftime('%Y-%m-%d')}_p{page}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return filename


def main():
    # Configuração
    config = APIConfig(
        base_url='https://crm.rdstation.com/api/v1/deals',
        token='Token_fixo'  # Token fixo
    )

    output_dir = Path(
        r'pasta_de_destino\Nome_da_pasta'
    )

    client = RDStationClient(config)
    exporter = DataExporter(output_dir)

    start_date = datetime(2024, 7, 31)
    end_date = datetime.now()

    current_date = start_date
    while current_date <= end_date:
        page = 1
        while True:
            data = client.fetch_deals(current_date, page)
            if not data:
                break

            file_path = exporter.save_deals(data, current_date, page)
            logging.info(f"💾 Salvo: {file_path}")

            # Paginação segura — para quando não houver mais registros
            registros = data if isinstance(data, list) else data.get('deals', [])
            if not registros or len(registros) < config.per_page:
                logging.info(f"📋 Todas as páginas de {current_date.strftime('%d/%m/%Y')} processadas")
                break

            page += 1

        current_date += timedelta(days=1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"❌ Erro inesperado: {e}")
