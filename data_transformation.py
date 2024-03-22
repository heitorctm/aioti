from datetime import datetime
import json

def extrair_dados_gerais(linha):
    try:
        dados_gerais = {}
        elementos = linha.split('&')
        for elemento in elementos:
            if elemento.startswith(('GET /cygmsrv/SFgm.ashx?ID', 'V', 'D', 'S')):
                chave, valor = elemento.split('=')
                if chave == 'GET /cygmsrv/SFgm.ashx?ID':
                    dados_gerais['log'] = int(valor)  
                elif chave in ('V', 'S'):
                    dados_gerais[chave] = int(valor)
                elif chave == 'D':
                    data, hora = valor.split('-')[:3], valor.split('-')[3:]
                    hora_formatada = ':'.join(hora) 
                    dados_gerais['date'] = datetime.strptime('-'.join(data), "%Y-%m-%d").strftime('%Y-%m-%d')
                    dados_gerais['time'] = datetime.strptime(hora_formatada, "%H:%M:%S").strftime('%H:%M:%S')
        return dados_gerais
    except Exception as e:
        print(f"Erro ao extrair dados gerais: {e}")
        return {}

def linha_para_json_separado(linha):
    try:
        dados_gerais = extrair_dados_gerais(linha)
        if not dados_gerais:
            return [], [] 

        elementos = linha.split('&')
        dados_inv = []
        dados_str = []

        for elemento in elementos:
            if elemento.startswith('INV'):
                dados_inv.append(json.dumps(transformar_dados_inversor(elemento, dados_gerais), ensure_ascii=False))
            elif elemento.startswith('STR'):
                dados_str.append(json.dumps(transformar_dados_string(elemento, dados_gerais), ensure_ascii=False))

        return dados_inv, dados_str
    except Exception as e:
        print(f"Erro durante a transformação da linha para JSON: {e}")
        return [], []


def transformar_dados_inversor(elemento, dados_gerais):
    chave, valores = elemento.split('=')
    inversor_id = int(chave[3:])
    valores_lista = valores.split(';')

    dados_inversor = {
        'inversor_id': inversor_id,
        'log': dados_gerais['log'],
        'version': dados_gerais['V'],
        'data': dados_gerais['date'],
        'hora': dados_gerais['time'],
        'cod': dados_gerais['S'],
        'status': int(valores_lista[0]),
        'energia_total_produzida': int(valores_lista[1]),
        'chave_1': int(valores_lista[2]),
        'chave_2': int(valores_lista[3]),
        'RmsVoltageRS': int(valores_lista[4]),
        'RmsVoltageST': int(valores_lista[5]),
        'RmsVoltageTR': int(valores_lista[6]),
        'RmsVoltageR': int(valores_lista[7]),
        'RmsVoltageS': int(valores_lista[8]),
        'RmsVoltageT': int(valores_lista[9]),
        'RmsCurrentR': int(valores_lista[10]),
        'RmsCurrentS': int(valores_lista[11]),
        'RmsCurrentT': int(valores_lista[12]),
        'CurrentImbalance': int(valores_lista[13]),
        'ActivePowerR': int(valores_lista[14]),
        'ActivePowerS': int(valores_lista[15]),
        'ActivePowerT': int(valores_lista[16]),
        'ReactivePowerR': int(valores_lista[17]),
        'ReactivePowerS': int(valores_lista[18]),
        'ReactivePowerT': int(valores_lista[19]),
        'Media_tensao': int(valores_lista[20]),
        'Media_corrente': int(valores_lista[21]),
        'PVpower': int(valores_lista[22]),
        'OperatingTime': int(valores_lista[23]),
        'SupplyTime': int(valores_lista[24]),
        'freq': int(valores_lista[25]),
        'Temperatura': int(valores_lista[26]),
        'status_log': int(valores_lista[27]),
        'eficiencia': int(valores_lista[28]),
        'producao_diaria': int(valores_lista[29]),
        'modo_palavra': valores_lista[30]
    }

    return dados_inversor

def transformar_dados_string(elemento, dados_gerais):
    chave, valores = elemento.split('=')
    string_id = int(chave[3:])
    valores_lista = valores.split(';')

    dados_string = {
        'string_id': string_id,
        'log': dados_gerais['log'],
        'version': dados_gerais['V'],
        'data': dados_gerais['date'],
        'hora': dados_gerais['time'],
        'cod': dados_gerais['S'],
        'valor': int(valores_lista[0]),
        'string_current1': int(valores_lista[1]),
        'string_current2': int(valores_lista[2]),
        'string_current3': int(valores_lista[3]),
        'string_current4': int(valores_lista[4]),
        'string_current5': int(valores_lista[5]),
        'string_current6': int(valores_lista[6]),
        'string_current7': int(valores_lista[7]),
        'string_current8': int(valores_lista[8]),
        'current_mean_value': int(valores_lista[9]),
        'current_max_value': int(valores_lista[10]),
        'current_min_value': int(valores_lista[11]),
        'board_temp': int(valores_lista[12]),
        'warn_int1': int(valores_lista[13]),
        'warn_int2': int(valores_lista[14]),
        'status_log': int(valores_lista[15])
                
        }
    
    return dados_string

