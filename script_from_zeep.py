from zeep import Client
from zeep.transports import Transport
from requests.auth import HTTPBasicAuth

# URL del servicio SOAP
url = 'http://45.224.186.20:59491/ReportarMetricas.svc'

# Nombre de usuario y contraseña para la autenticación básica
username = 'test'
password = 'pass'

# Crear un cliente SOAP con autenticación básica
transport = Transport()
transport.session.auth = HTTPBasicAuth(username, password)
client = Client(url, transport=transport)

# Construir la solicitud SOAP
soap_request = {
    'body': {
        'Metricas': {
            'Metrica': {
                'Link': '6186P42',
                'UnidadMedida': 'TON',
                'PuntoControl': '65',
                'ValorVariableRecopilada': '1',
                'FechaTomaMedida': '2020-12-17 15:17:46.000',
                'TipoMaterial': 'ARENA',
                'TipoVariable': 'PESO'
            }
        }
    }
}

# Invocar el método del servicio SOAP
response = client.service.ReportarMetrica(**soap_request)

# Imprimir la respuesta
print(response)