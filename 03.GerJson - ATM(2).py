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
        """
        Busca oportunidades do RD Station com sistema de retry
        para uma data e página específicas
        """
        # Define horário inicial e final do mesmo dia
        start_datetime = date.replace(hour=0, minute=0, second=1)
        end_datetime = date.replace(hour=23, minute=59, second=59)
        
        params = {
            'token': self.config.token,
            'created_at_period  ': True,
            'start_date': start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            'end_date': end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            'page': page,
            'per_page': self.config.per_page
        }

        # Tenta fazer a requisição com retry
        for attempt in range(self.config.retry_attempts):
            try:
                response = requests.get(
                    self.config.base_url, 
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                
                print(f"✅ Dados do dia {date.strftime('%d/%m/%Y')} (Página {page}) obtidos com sucesso - {self.config.base_url}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Tentativa {attempt + 1} falhou para o dia {date.strftime('%d/%m/%Y')} (Página {page}): {str(e)} - {self.config.base_url}")
                if attempt < self.config.retry_attempts - 1:
                    print(f"⏳ Aguardando {self.config.retry_delay} segundos para nova tentativa...")
                    sleep(self.config.retry_delay)
                else:
                    print(f"❌ Falha ao obter dados após {self.config.retry_attempts} tentativas")
                    return None

class DataExporter:
    """Gerencia a exportação dos dados para arquivos"""
    def __init__(self, output_directory: str):
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_deals(self, data: Dict, date: datetime, page: int) -> Path:
        """Salva os dados das oportunidades em arquivo JSON"""
        filename = self.output_dir / f"oportunidades_{date.strftime('%Y-%m-%d')}_p{page}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return filename

def main():
    # Configuração inicial
    config = APIConfig(
        base_url='https://crm.rdstation.com/api/v1/deals',
        token='66259070c34464001534835a'
    )

    # Diretório para salvar os dados
    output_dir = Path(r'G:\.shortcut-targets-by-id\1kArAZwgCxrjbQwQOPEzeJLtMUll3VVJ7\13. Dados\13.9. Atendimento\13.9.2. RD Station\13.9.2.1. Oportunidade')
    
    client = RDStationClient(config)
    exporter = DataExporter(output_dir)
    
    # Define período de coleta
    start_date = datetime(2024, 7, 1)
    end_date = datetime.now()
    
    # Loop principal
    current_date = start_date
    while current_date <= end_date:
        page = 1
        
        while True:
            # Busca dados da API
            data = client.fetch_deals(current_date, page)
            
            if not data:
                break
                
            # Salva os dados no arquivo
            file_path = exporter.save_deals(data, current_date, page)
            print(f"✅ Dados do dia {current_date.strftime('%d/%m/%Y')} (Página {page}) salvos em: {file_path}")
            
            # Verifica se há mais páginas
            if not data.get("has_more", False):
                print(f"📋 Todas as páginas do dia {current_date.strftime('%d/%m/%Y')} foram processadas")
                break
                
            # Avança para próxima página
            page += 1
            
        # Avança para o próximo dia
        current_date += timedelta(days=1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Erro inesperado: {str(e)}")