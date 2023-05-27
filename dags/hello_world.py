from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from discordwebhook import Discord
import cfscrape
import json
import time
import colorama
from datetime import datetime
from bs4 import BeautifulSoup

from types import ModuleType
from inspect import currentframe, getmodule
import sys

colorama.init()

REQUESTS_MANAGER = cfscrape.CloudflareScraper()
GET = REQUESTS_MANAGER.get
POST = REQUESTS_MANAGER.post
JSON_TO_TABLE = json.loads
TABLE_TO_JSON = json.dumps
COLOR = colorama.Fore

DISCORD_BASIC_LOGGING = False

LOGGING_WEBHOOK = 'https://discord.com/api/webhooks/1111330702353518683/A-1GbjNAXqvdGnCLflrS-zuvNha1Jyoavwc2TSrNca5WuTNhAhCmkoa-nzqLFXIKIGMJ'

WEBHOOKS = [
    # You can add as many webhooks as u want, diving them with ","
    'https://discord.com/api/webhooks/1111330702353518683/A-1GbjNAXqvdGnCLflrS-zuvNha1Jyoavwc2TSrNca5WuTNhAhCmkoa-nzqLFXIKIGMJ',
]

COUNTRY_LINKS = {
    'IT': 'https://www.zalando.es/calzado-hombre/adidas.nike/',
    'UK': 'https://www.zalando.es/calzado-hombre/adidas.nike/'
}

COUNTRY_BASE_URL = {
    'IT': 'https://www.zalando.es/calzado-hombre/adidas.nike/',
    'UK': 'https://www.zalando.es/calzado-hombre/adidas.nike/'
}

LOGGING_COLORS = {
    "INFO": COLOR.CYAN,
    "LOG": COLOR.BLUE,
    "WARNING": COLOR.YELLOW,
    "ERROR": COLOR.RED,
}

def log(logType, message, details):
    logDate = str(datetime.now())

    logFile = open('logs.log', 'a+')

    if len(details) == 0:
        logFile.write(logDate + ' [%s] ' % (logType) + message + '\n')
        print(logDate + LOGGING_COLORS[logType] + ' [%s] ' %
              (logType) + message + COLOR.RESET)
    else:
        logFile.write(logDate + ' [%s] ' % (logType) +
                      message + ' | ' + TABLE_TO_JSON(details) + '\n')
        print(logDate + LOGGING_COLORS[logType] + ' [%s] ' %
              (logType) + message + ' | ' + TABLE_TO_JSON(details) + COLOR.RESET)

    logFile.close()

    detailsString = ''

    if (logType == 'LOG') and (DISCORD_BASIC_LOGGING == False):
        return

    for x in details:
        detailsString += '`%s = %s`\n' % (str(x), details[x])

    data = {
        "content": None,
        "embeds": [
            {
                "color": None,
                "fields": [
                    {
                        "name": "LOG TYPE",
                        "value": "`%s`" % (logType)
                    },
                    {
                        "name": "LOG MESSAGE",
                        "value": "`%s`" % (message)
                    },
                    {
                        "name": "LOG DETAILS",
                        "value": detailsString
                    },
                    {
                        "name": "TIME",
                        "value": "`%s`" % (logDate)
                    }
                ]
            }
        ],
        "username": "Zalando Scraper Logs",
        "avatar_url": "https://avatars.githubusercontent.com/u/1564818?s=280&v=4"
    }

    if len(details) == 0:
        data['embeds'][0]['fields'].remove(data['embeds'][0]['fields'][2])

    POST(LOGGING_WEBHOOK, json=data)


def save_external_articles(content):
    file = open('articles.json', 'w+')
    file.write(TABLE_TO_JSON(content))
    file.close()
    return content


def load_external_articles():
    print("-load_external_articles-")
    open('articles.json', 'a+')
    print("-line2-")
    file = open('articles.json', 'r')
    print("-line3-")
    fileContent = file.read()
    print("-line4-")
    if len(fileContent) < 2:
        save_external_articles([])
        return []
    try:
        file.close()
        return JSON_TO_TABLE(fileContent)
    except:
        save_external_articles([])
        return []


def validate_country(countryCode):
    return not (COUNTRY_LINKS[countryCode] == None)


def get_page_data(countryCode):

    if validate_country(countryCode):
        response = GET(COUNTRY_LINKS[countryCode])
        if response.status_code == 200:
            return response.content
        else:
            log('ERROR', 'Error while retrieving page',
                {'statusCode': response.status_code})
            return {'error': 'Invalid Status Code', 'status_code': response.status_code}
    log('ERROR', 'Invalid Country (get_page_data)',
        {'countryCode': countryCode})
    return {'error': 'Invalid Country'}

def filter_json(content):
    vectorArticulos = []
    soup = BeautifulSoup(content, "html.parser")

    # Encontrar todos los elementos que contienen información de los productos
    product_items = soup.find_all("article", class_="z5x6ht _0xLoFW JT3_zV mo6ZnF _78xIQ-")
    
    # Iterar sobre cada producto y extraer los datos relevantes
    for item in product_items:
        # Extraer el nombre del producto
        name = item.find("h3", class_="_6zR8Lt lystZ1 FxZV-M _4F506m ZkIJC- r9BRio qXofat EKabf7 nBq1-s _2MyPg2")
        
        # Extraer el precio del producto
        price = item.find("span", class_="ZiDB59 r9BRio uVxVjw")

        # Extraer los detalles del producto
        detalles = item.find("h3", class_="KxHAYs lystZ1 FxZV-M _4F506m ZkIJC- r9BRio qXofat EKabf7 nBq1-s _2MyPg2")

        if name != None and price != None and detalles != None:
            name_cadena = name.text.strip()
            price_cadena = price.text.strip()
            detalles_cadena = detalles.text.strip()
        
            # Imprimir los datos del producto
            print("-----------------------------producto----------------------------------")
            print("Nombre: ", name_cadena)
            print("Precio: ", price_cadena)
            print("Detalles: ", detalles_cadena)
            print("------------------------------------------------------------------------------")

            vectorArticulos.append({
                "name": name_cadena,
                "price": price_cadena,
                "detalles": detalles_cadena
            })
    
    return vectorArticulos

def adjust_articles_info(content, countryCode):
    adjustedArticlesList = []
    for article in content:
        articleInfo = {}
        rSplit = article['availability']['releaseDate'].split(' ')
        rDate = rSplit[0].split('-')
        rTime = rSplit[1]
        articleInfo['zalandoId'] = article['id']
        articleInfo['releaseDate'] = '%s-%s-%s %s' % (
            rDate[2], rDate[1], rDate[0], rTime)
        articleInfo['productName'] = article['brand'] + ' ' + article['name']
        articleInfo['originalPrice'] = article['price']['original']
        articleInfo['currentPrice'] = article['price']['current']
        articleInfo['link'] = "%s%s.html" % (
            COUNTRY_BASE_URL[countryCode], article['urlKey'])
        articleInfo['imageUrl'] = article['imageUrl']

        adjustedArticlesList.append(articleInfo)

    return adjustedArticlesList


def compare_articles(articles):
    if len(oldArticles) == 0:
        return articles
    else:
        if len(articles) == len(oldArticles):
            return []
        else:
            articlesToReturn = []
            for article in articles:
                found = False

                for article_ in oldArticles:

                    if article['zalandoId'] == article_['zalandoId']:
                        found = True

                if found == False:
                    articlesToReturn.append(article)

            return articlesToReturn


def get_product_stock(link):
    response = GET(link)
    bs = BeautifulSoup(response.content, 'html.parser')
    sizeArray = []
    try:
        sizeArray = JSON_TO_TABLE(bs.find("script", {'id': 'z-vegas-pdp-props'}).contents[0][9:-3])['model']['articleInfo']['units']
    except:
        log('ERROR','Could not retrieve model units and sizes',{'URL' : link})

    sizeStockArray = []
    for x in sizeArray:
        sizeStockArray.append({
            'size': x['size']['local'],
            'sizeCountry': x['size']['local_type'],
            'stock': x['stock']
        })

    return sizeStockArray


def send_message(content):

    for article in content:

        print("-ARTICULO-")
        print(article)
        print(article['name'])

        for webhook in WEBHOOKS:

            #log('LOG', 'Article Message Sent', {'WEBHOOK': webhook, 'ARTICLE-ID': article['name']})
            cadena_mensaje = article['name'] + " - " + article['detalles'] + " - " + article['price']
            #POST(webhook, content=cadena_mensaje)

            discord = Discord(url=webhook)
            discord.post(content=cadena_mensaje)


#oldArticles = load_external_articles()


def print_hello():
    global oldArticles
    country = 'IT'
    print("-data-")
    print(get_page_data(country))
    print("--------------------------")
    #articles = adjust_articles_info(filter_coming_soon(filter_articles(filter_json(get_page_data(country)))), country)
    articles = filter_json(get_page_data(country))
    #newArticles = compare_articles(articles)
    send_message(articles)
    #save_external_articles(articles)
    #oldArticles = articles




dag = DAG('hello_world', description='Hola Mundo DAG',
schedule_interval='* * * * *',
start_date=datetime(2017, 3, 20),
catchup=False,)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)