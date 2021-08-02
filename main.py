import json
import datetime
import time
import requests
import pytz
from bs4 import BeautifulSoup as BS

# Comment or uncomment categories
list_of_categories = (
    # 'Accesorios para Vehículos',      # Vehicle Accessories
    # 'Agro',                           # Agro
    # 'Alimentos y Bebidas',            # Food and drinks
    # 'Animales y Mascotas',            # Animals and Pets
    # 'Antigüedades y Colecciones',     # Antiques and Collections
    # 'Arte, Librería y Mercería',      # Art, Bookstore and Haberdashery
    # 'Autos, Motos y Otros',           # Cars, Motorcycles and Others
    # 'Bebés',                          # Babies
    # 'Belleza y Cuidado Personal',     # Beauty and Personal Care
    # 'Cámaras y Accesorios',           # Cameras and Accessories
    # 'Celulares y Teléfonos',          # Cell Phones and Phones
    'Computación',  # Computing
    # 'Consolas y Videojuegos',         # Consoles and Videogames
    # 'Construcción',                   # Building
    # 'Deportes y Fitness',             # Sports and Fitness
    # 'Electrodomésticos y Aires Ac.',  # Appliances and Aires Ac.
    # 'Electrónica, Audio y Video',     # Electronics, Audio and Video
    # 'Entradas para Eventos',          # Event Tickets
    'Herramientas',                     # Tools
    'Hogar, Muebles y Jardín',          # Home, Furniture and Garden
    'Industrias y Oficinas',            # Industries and Offices
    'Inmuebles',                        # Estate
    'Instrumentos Musicales',           # Musical instruments
    'Joyas y Relojes',                  # Jewelry and watches
    'Juegos y Juguetes',                # Games and toys
    'Libros, Revistas y Comics',        # Books, Magazines and Comics
    'Música, Películas y Series',       # Music, Movies and Series
    # 'Ropa y Accesorios',              # Clothes and accessories
    # 'Salud y Equipamiento Médico',    # Health and Medical Equipment
    # 'Servicios',                      # Services
    # 'Souvenirs, Cotillón y Fiestas',  # Souvenirs, Party Favors and Parties
    # 'Otras categorías'                # Other categories
)

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
}

proxy_list = [
    {
        "http": "http://demoend:test123@34.138.226.3:3128/",  # USA
        "https": "http://demoend:test123@34.138.226.3:3128"
    },
    {}  # local
]

session.headers.update(headers)
root = 'https://www.mercadolibre.com.ar/categorias#menu=categories'


#########################################################################

def proxy_rotation(list_of_proxy):
    while True:
        for proxy in list_of_proxy:
            yield proxy


def request(url, retry=5):
    """Getting HTML, 403 error prevention"""
    try:
        # time.sleep(1.8)
        response = session.get(url)
        proxy = proxy_rotation(proxy_list)
        while response.status_code == 403:
            print('Error 403. IP switching')
            updated_proxy = next(proxy)
            if updated_proxy:
                session.proxies.update(updated_proxy)
                response = session.get(url)
            else:  # local ip
                session.proxies.clear()
                response = session.get(url)
    except Exception as ex:
        time.sleep(3)
        print(f'{retry=},{url=},{ex=}')
        if retry:
            return request(url, retry=retry - 1)
        else:
            raise
    else:
        return response.text


def get_categories_links(selected_categories):
    """Getting titles and links of categories"""
    response = request(root)
    soup = BS(response, 'lxml')
    list_of_links_to_categories = soup.find_all('div', class_='categories__container')
    for main_category in list_of_links_to_categories:
        title_of_main_category = main_category.find('h2').get_text(strip=True)
        if selected_categories:
            if title_of_main_category in selected_categories:
                subcategories_list = main_category.find_all('li', class_='categories__item')
                for subcategory in subcategories_list:
                    title_of_subcategory = subcategory.find('h3', class_='categories__subtitle-title').get_text(
                        strip=True)
                    link_to_subcategory = subcategory.find('a').attrs['href']
                    yield title_of_main_category, title_of_subcategory, link_to_subcategory
        else:  # if selected_categories = ()
            subcategories_list = main_category.find_all('li', class_='categories__item')
            for subcategory in subcategories_list:
                title_of_subcategory = subcategory.find('h3', class_='categories__subtitle-title').get_text(strip=True)
                link_to_subcategory = subcategory.find('a').attrs['href']
                yield title_of_main_category, title_of_subcategory, link_to_subcategory


def getting_links_to_items(link_to_subcategory):
    """Iteration through pagination and getting links to items"""
    response = request(link_to_subcategory + '_DisplayType_LF')
    soup = BS(response, 'lxml')
    items = soup.find_all('li', {'class': 'ui-search-layout__item'})
    for item in items:
        item_link = item.find('a').attrs['href']
        yield item_link
    n = 51
    response = request(f'{link_to_subcategory}_Desde_{n}_DisplayType_LF')
    soup = BS(response, 'lxml')
    pagination_arrow = soup.find_all('span', {'class': 'andes-pagination__arrow-title'})
    while len(list(pagination_arrow)) > 1:
        items = soup.find_all('li', {'class': 'ui-search-layout__item'})
        for item in items:
            item_link = item.find('a').attrs['href']
            yield item_link
        n += 50
        response = request(f'{link_to_subcategory}_Desde_{n}_DisplayType_LF')
        soup = BS(response, 'lxml')
        pagination_arrow = soup.find_all('span', {'class': 'andes-pagination__arrow-title'})


def parse_data(item_link, title_of_main_category, title_of_subcategory):
    """Parse items"""
    response = request(item_link)
    soup = BS(response, 'lxml')
    try:
        data_raw = soup.find('script', {'type': 'application/ld+json'}).string
    except:
        return None
    data_raw = json.loads(data_raw)
    try:
        availability = data_raw['offers'].get('availability').replace('http://schema.org/', '')
    except:
        availability = None
    try:
        currency = data_raw['offers'].get('priceCurrency')
    except:
        currency = None
    try:
        url = data_raw['offers'].get('url')
    except:
        url = None
    data = {
        'item': {
            'source': 'Mercado_Libre_ARG',
            'sku': data_raw.get('productID'),
            'name': data_raw.get("name").strip(),
            'currency': currency,
            'category': title_of_main_category,
            'subcategory': title_of_subcategory,
            'brand': data_raw.get('brand'),
            'country': 'ARG',
            'availability': availability,
            'url': url,
            'upc': data_raw.get("gtin8"),
            'image_url': data_raw.get('image'),
        },
        'price': {
            'price': data_raw['offers'].get('price'),
            'observed_date': str(pytz.utc.localize(datetime.datetime.utcnow()))
        }
    }
    return data


def main():
    all_objects = []
    try:
        n = 0
        for category in get_categories_links(list_of_categories):
            print(category[2])
            for item in getting_links_to_items(category[2]):
                get_data = parse_data(item, title_of_main_category=category[0],
                                      title_of_subcategory=category[1])
                if get_data:
                    all_objects.append(get_data)
                n += 1
                print(n, get_data)
    except Exception as ex:
        print(ex)
    finally:
        print(f'saving to output_json/data-{str(datetime.datetime.now())[:-7]}.json')
        with open(f'output_json/data-{str(datetime.datetime.now())[:-7]}.json', 'w', encoding='utf-8') as json_file:
            json.dump(all_objects, json_file, indent=4, separators=(',', ': '), ensure_ascii=False)


if __name__ == '__main__':
    main()
