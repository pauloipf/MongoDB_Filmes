""" 
Consulta a API TMDb para obter informações detalhadas de filmes, incluindo: gênero, diretor, ator principal e atores coadjuvantes.
"""

import time
import json
import os
from datetime import datetime
import requests
from pymongo import MongoClient

# Configurações da API
API_KEY = "YOUR API KEY HERE"
BASE_URL = 'https://api.themoviedb.org/3'
LANGUAGE = 'pt-BR'
# Read starting page from pages.txt or default to 1
try:
    with open('page.txt', 'r') as f:
        START_PAGE = int(f.read().strip())
except FileNotFoundError:
    START_PAGE = 1
    with open('page.txt', 'w') as f:
        f.write(str(START_PAGE))

END_PAGE = 500      # Final page for query

# Configurações do MongoDB
MONGO_URI = 'YOUR MONGODB URI HERE'
DB_NAME = 'cinema'
COLLECTION_NAME = 'filmes'

# Configurações de cabeçalho para API
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Accept': 'application/json'
}

def connect_to_mongodb():
    """
    Conecta ao MongoDB e retorna a coleção.
    
    Returns:
        tuple: Uma tupla contendo o cliente MongoDB e a coleção de filmes.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Criar coleção se não existir
    if COLLECTION_NAME not in db.list_collection_names():
        print(f"Criando nova coleção: {COLLECTION_NAME}")

    collection = db[COLLECTION_NAME]
    return client, collection

def get_json(url, params=None, max_retries=3):
    """
    Obtém dados JSON da API com tratamento de erros e tentativas.
    
    Args:
        url (str): URL da API a ser consultada.
        params (dict, optional): Parâmetros da requisição. Defaults to None.
        max_retries (int, optional): Número máximo de tentativas. Defaults to 3.
        
    Returns:
        dict: Dados JSON retornados pela API ou None em caso de falha.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get('Retry-After', 180))
                print(f'\nAPI bloqueou a conexão. Aguardando {retry_after} segundos antes de tentar novamente...')
                time.sleep(retry_after)
            else:
                print(f'\nErro ao acessar {url}: {response.status_code}')
                time.sleep(5)  # Pequena pausa antes de tentar novamente
        except requests.exceptions.RequestException as e:
            print(f'\nErro de conexão: {e}')
            print(f'Tentativa {attempt+1} de {max_retries}. Aguardando 180 segundos...')
            time.sleep(180)  # Aguardar 3 minutos antes de tentar novamente

    print(f'Falha após {max_retries} tentativas para {url}')
    return None

def get_person_details(person_id):
    """
    Obtém detalhes de uma pessoa (ator/diretor).
    
    Args:
        person_id (int): ID da pessoa na API TMDb.
        
    Returns:
        dict: Informações da pessoa ou None se não encontrada.
    """
    url = f'{BASE_URL}/person/{person_id}'
    params = {'language': LANGUAGE}
    data = get_json(url, params)
    if data:
        nome = data.get('name')
        nacionalidade = data.get('place_of_birth', 'Desconhecida')
        data_nascimento = data.get('birthday')
        if data_nascimento:
            idade = datetime.now().year - int(data_nascimento[:4])
        else:
            idade = None
        return {
            'nome': nome,
            'idade': idade,
            'nacionalidade': nacionalidade
        }
    return None

def process_page(page, end_page):
    """
    Processa uma página de filmes e retorna os dados.
    
    Args:
        page (int): Número da página a ser processada.
        end_page (int): Número total de páginas para exibição de progresso.
        
    Returns:
        list: Lista de dicionários com informações dos filmes.
    """
    print(f'Processando página {page} de {end_page} ', end='', flush=True)

    params = {'language': LANGUAGE, 'page': page}
    top_rated = get_json(f'{BASE_URL}/movie/top_rated', params)
    if not top_rated:
        print("\nFalha ao obter lista de filmes. Pulando página.")
        return []

    page_movies = []

    for movie in top_rated.get('results', []):
        try:
            movie_id = movie['id']

            # Detalhes do filme
            movie_details = get_json(f'{BASE_URL}/movie/{movie_id}', {'language': LANGUAGE})
            if not movie_details:
                continue

            # Créditos do filme
            credits = get_json(f'{BASE_URL}/movie/{movie_id}/credits', {'language': LANGUAGE})
            if not credits:
                continue

            # Gênero
            generos = movie_details.get('genres', [])
            genero = generos[0]['name'] if generos else 'Desconhecido'

            # Diretor
            diretor = next((member['name'] for member in credits.get('crew', []) if member['job'] == 'Director'), 'Desconhecido')

            # Ator principal
            elenco = credits.get('cast', [])
            ator_principal = elenco[0] if elenco else None
            if ator_principal:
                ator_principal_details = get_person_details(ator_principal['id'])
            else:
                ator_principal_details = None

            # Atores coadjuvantes
            atores_coadjuvantes = [actor['name'] for actor in elenco[1:5]] if len(elenco) > 1 else []

            # Montar estrutura
            filme_info = {
                'titulo': movie_details.get('title'),
                'ano': int(movie_details.get('release_date', '0000-00-00')[:4]) if movie_details.get('release_date') else None,
                'genero': genero,
                'diretor': diretor,
                'nota': movie_details.get('vote_average'),
                'dataLancamento': movie_details.get('release_date'),
                'atorPrincipal': ator_principal_details,
                'atoresCoadjuvantes': atores_coadjuvantes
            }

            page_movies.append(filme_info)
            print("█", end='', flush=True)  # Indicador de progresso

        except Exception as e:
            print(f"\nErro ao processar filme {movie_id if 'movie_id' in locals() else 'desconhecido'}: {e}")

        time.sleep(0.05)  # Pequena pausa entre filmes

    print()  # Nova linha após processar todos os filmes da página
    return page_movies

def save_to_mongodb(movies):
    """
    Salva os filmes no MongoDB.
    
    Args:
        movies (list): Lista de dicionários com informações dos filmes.
        
    Returns:
        int: Número de novos filmes adicionados.
    """
    if not movies:
        print("Nenhum filme para salvar no MongoDB.")
        return 0

    try:
        client, collection = connect_to_mongodb()

        # Verificar filmes existentes para evitar duplicatas
        titulos_existentes = {doc['titulo'] for doc in collection.find({}, {'titulo': 1})}

        # Filtrar apenas filmes novos
        novos_filmes = [filme for filme in movies if filme['titulo'] not in titulos_existentes]

        if novos_filmes:
            collection.insert_many(novos_filmes)
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"[{current_time}] Adicionados {len(novos_filmes)} novos filmes ao MongoDB.")
        else:
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"[{current_time}] Nenhum filme novo para adicionar.")

        # Fechar conexão
        client.close()
        return len(novos_filmes)

    except Exception as e:
        print(f"Erro ao salvar no MongoDB: {e}")
        return 0

def main():
    """
    Função principal que gerencia o processo de download e carregamento.
    
    Coordena o processo de busca de filmes da API TMDb e salvamento no MongoDB,
    com tratamento de erros e tentativas de reconexão.
    """
    print(f"Iniciando processamento de filmes do TMDb em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    total_filmes = 0

    for page in range(START_PAGE, END_PAGE + 1):
        # Processar uma página por vez
        page_movies = []
        max_retries = 3

        for attempt in range(max_retries):
            try:
                page_movies = process_page(page, END_PAGE)
                break  # Se bem-sucedido, sai do loop de tentativas
            except Exception as e:
                print(f"\nErro ao processar página {page}: {e}")
                if attempt < max_retries - 1:
                    print(f"Tentativa {attempt+1} de {max_retries}. Aguardando 180 segundos...")
                    time.sleep(180)  # Aguardar 3 minutos antes de tentar novamente
                else:
                    print(f"Falha após {max_retries} tentativas. Pulando para a próxima página.")

        # Salvar filmes desta página no MongoDB
        if page_movies:
            novos_filmes = save_to_mongodb(page_movies)
            total_filmes += novos_filmes
            
        # Atualizar o arquivo page.txt com a página atual processada
        with open('page.txt', 'w') as f:
            f.write(str(page+1))
        print(f"Progresso salvo: página {page}")

        time.sleep(1)  # Pequena pausa entre páginas

    print(f"\nProcessamento concluído em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total de filmes adicionados ao MongoDB: {total_filmes}")

if __name__ == '__main__':
    main()
