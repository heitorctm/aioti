import socket
import select
import time
from data_transformation import linha_para_json_separado
import database
from env import ips_permitidos

def iniciar_servidor(host='0.0.0.0', porta=8080, timeout=10):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, porta))
        s.listen()
        print(f"Servidor escutando em {host}:{porta}")

        while True:
            try:
                conn, addr = s.accept()
            except socket.error as e:
                print(f"Erro ao aceitar conexão: {e}")
                continue

            ip_cliente = addr[0]
            if ip_cliente not in ips_permitidos:
                print(f"Acesso negado para {ip_cliente}")
                conn.close()
                continue

            with conn:
                print(f"Conectado por {addr}")
                data_buffer = ""
                ultima_recepcao = time.time()

                conn.setblocking(0)  # modo nao bloqueante

                try:
                    while True:
                        pronto_para_receber, _, _ = select.select([conn], [], [], timeout)

                        if pronto_para_receber:
                            try:
                                dados = conn.recv(4096)
                            except socket.error as e:
                                print(f"Erro ao receber dados: {e}")
                                break

                            if not dados:
                                break  
                            data_buffer += dados.decode('utf-8')
                            ultima_recepcao = time.time()

                            if '\n' in data_buffer:
                                dados_inv, dados_str = linha_para_json_separado(data_buffer.strip())
                                data_buffer = ""

                                # funções de inserção do banco de dados
                                database.verificar_ou_inserir_strings(dados_str)
                                database.inserir_no_banco(dados_inv, 'inversor')
                                database.inserir_no_banco(dados_str, 'string')
                                print('Dados inseridos no banco com sucesso.')

                                try:
                                    resposta = 'true'
                                    dados_para_enviar = resposta.encode('utf-8')
                                    conn.sendall(dados_para_enviar)
                                except socket.error as e:
                                    print(f"Erro ao enviar resposta: {e}")
                                    break

                                break
                        else:
                            print("Timeout.")
                            break

                        if time.time() - ultima_recepcao > timeout:
                            print("Timeout.")
                            break

                finally:
                    conn.close()
                    print("Conexão fechada com", addr)

if __name__ == "__main__":
    iniciar_servidor()
