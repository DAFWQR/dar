import logging
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import re
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
import asyncio
import multiprocessing

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tennis_parser.log'),
        logging.StreamHandler()
    ]
)

class TennisParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://www.flashscorekz.com"
        self.url = f"{self.base_url}/tennis/"
        self.live_url = f"{self.base_url}/live/tennis/"
        self.setup_driver()

    def setup_driver(self):
        """Настройка Firefox WebDriver"""
        try:
            start_time = time.time()
            self.logger.info("Начало инициализации WebDriver...")
            
            firefox_options = Options()
            firefox_options.add_argument('--headless')
            firefox_options.set_preference('network.http.connection-timeout', 10)
            firefox_options.set_preference('network.http.response-timeout', 10)
            firefox_options.binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'
            
            service = Service(executable_path='geckodriver.exe')
            self.driver = webdriver.Firefox(service=service, options=firefox_options)
            self.driver.set_page_load_timeout(10)  # Ограничение времени загрузки страницы
            
            end_time = time.time()
            self.logger.info(f"WebDriver успешно инициализирован за {end_time - start_time:.2f} секунд")
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации WebDriver: {str(e)}")
            raise

    def get_participant_name(self, elem):
        """Универсально собирает имя игрока или команды (включая парные/командные матчи), разделяет длинным тире"""
        try:
            parts = []
            for child in elem.find_elements(By.XPATH, ".//*"):
                txt = child.text.strip()
                if txt:
                    parts.append(txt)
            if not parts:
                return elem.text.strip()
            return " — ".join(parts)
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении имени игрока: {e}")
            return elem.text.strip()

    def get_match_links(self):
        """Получение ссылок и игроков для live матчей (универсально для одиночных, парных, командных, поддержка нескольких ссылок в контейнере)"""
        try:
            self.logger.info(f"Загрузка специальной страницы LIVE для тенниса: {self.live_url}")
            try:
                self.driver.get(self.live_url)
            except Exception as e:
                self.logger.error(f"Ошибка загрузки страницы {self.live_url}: {e}")
                return []
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "sportName"))
                )
                self.logger.info("Специальная страница LIVE для тенниса загружена")
            except Exception as e:
                self.logger.warning(f"Не удалось загрузить специальную страницу LIVE: {str(e)}")
                alt_live_url = f"{self.url}?type=live"
                self.logger.info(f"Пробуем альтернативную страницу LIVE: {alt_live_url}")
                try:
                    self.driver.get(alt_live_url)
                except Exception as e:
                    self.logger.error(f"Ошибка загрузки страницы {alt_live_url}: {e}")
                    return []
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "sportName"))
                    )
                    self.logger.info("Альтернативная страница LIVE загружена")
                except Exception as e:
                    self.logger.error(f"Не удалось загрузить альтернативную страницу LIVE: {str(e)}")
                    return []
            # Собираем контейнеры только live-матчей
            matches = []
            match_elements = self.driver.find_elements(By.CSS_SELECTOR, ".event__match.event__match--live")
            self.logger.info(f"Найдено {len(match_elements)} live-контейнеров матчей")
            for match in match_elements:
                try:
                    home_elem = match.find_element(By.CSS_SELECTOR, ".event__participant--home")
                    away_elem = match.find_element(By.CSS_SELECTOR, ".event__participant--away")
                    home = self.get_participant_name(home_elem)
                    away = self.get_participant_name(away_elem)
                    # Собираем все ссылки внутри контейнера
                    link_elems = match.find_elements(By.CSS_SELECTOR, "a[href*='match/']")
                    links = [a.get_attribute("href") for a in link_elems if a.get_attribute("href")]
                    if links and home and away:
                        matches.append({"urls": links, "home_player": home, "away_player": away})
                except Exception as e:
                    self.logger.error(f"Ошибка при сборе данных матча: {e}")
                    continue
            self.logger.info(f"Всего собрано {len(matches)} live-матчей с игроками и ссылками (возможно несколько ссылок на матч)")
            return matches
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка матчей: {str(e)}")
            return []

    def parse_serve_stats(self):
        """Парсинг статистики подачи"""
        start_time = time.time()
        self.logger.info("Начало парсинга статистики подачи...")
        
        try:
            serve_stats = {}
            
            # Ждем загрузки секции статистики
            self.logger.info("Ожидание загрузки секции статистики...")
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics']"))
                )
                self.logger.info("Секция статистики загружена")
            except Exception as e:
                self.logger.error(f"Не удалось дождаться секции статистики: {str(e)}")
                return {}
            
            # Находим секцию подачи - используем указанный пользователем селектор
            self.logger.info("Поиск секции подачи...")
            try:
                serve_section = self.driver.find_element(By.XPATH, 
                    "//div[@data-analytics-context='tab-match-statistics']//div[contains(@class, 'section')][.//div[contains(@class, 'sectionHeader') and contains(text(), 'Подача')]]")
                self.logger.info("Секция подачи найдена")
            except Exception as e:
                self.logger.error(f"Не удалось найти секцию подачи: {str(e)}")
                return {}
            
            # Парсим статистику подачи
            stats_rows = serve_section.find_elements(By.CSS_SELECTOR, ".wcl-row_OFViZ[data-testid='wcl-statistics']")
            self.logger.info(f"Найдено {len(stats_rows)} строк статистики подачи")
            
            for row in stats_rows:
                try:
                    # Извлекаем категорию
                    category = row.find_element(By.CSS_SELECTOR, ".wcl-category_7qsgP strong").text
                    
                    # Извлекаем значения для домашнего и гостевого игроков
                    home_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-homeValue_-iJBW")
                    away_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-awayValue_rQvxs")
                    
                    # Извлекаем основное значение (в strong)
                    home_main_value = home_value_element.find_element(By.CSS_SELECTOR, "strong").text
                    away_main_value = away_value_element.find_element(By.CSS_SELECTOR, "strong").text
                    
                    # Проверяем наличие дополнительных значений (в span)
                    home_detail_value = ""
                    away_detail_value = ""
                    
                    try:
                        spans_home = home_value_element.find_elements(By.CSS_SELECTOR, "span")
                        if spans_home and len(spans_home) > 0:
                            for span in spans_home:
                                if span.text and "(" in span.text:
                                    home_detail_value = span.text.strip("()")
                    except:
                        pass
                        
                    try:
                        spans_away = away_value_element.find_elements(By.CSS_SELECTOR, "span")
                        if spans_away and len(spans_away) > 0:
                            for span in spans_away:
                                if span.text and "(" in span.text:
                                    away_detail_value = span.text.strip("()")
                    except:
                        pass
                    
                    # Формируем структуру с данными
                    if home_detail_value or away_detail_value:
                        serve_stats[category] = {
                            "home": {"value": home_main_value, "details": home_detail_value},
                            "away": {"value": away_main_value, "details": away_detail_value}
                        }
                    else:
                        serve_stats[category] = {
                            "home": home_main_value,
                            "away": away_main_value
                        }
                    
                    self.logger.debug(f"Обработана статистика подачи: {category}")
                except Exception as e:
                    self.logger.error(f"Ошибка при парсинге строки статистики подачи: {str(e)}")
                    continue
            
            end_time = time.time()
            self.logger.info(f"Парсинг статистики подачи завершен за {end_time - start_time:.2f} секунд")
            return serve_stats
            
        except Exception as e:
            end_time = time.time()
            self.logger.error(f"Ошибка при парсинге статистики подачи: {str(e)}")
            self.logger.error(f"Время выполнения с ошибкой: {end_time - start_time:.2f} секунд")
            return {}

    def parse_game_stats(self):
        """Парсинг статистики возврата и очков"""
        start_time = time.time()
        self.logger.info("Начало парсинга статистики возврата и очков...")
        
        try:
            game_stats = {}
            
            # Находим секции возврата и очков
            self.logger.info("Поиск секций возврата и очков...")
            
            # Используем указанные пользователем селекторы
            sections = self.driver.find_elements(By.XPATH, 
                "//div[@data-analytics-context='tab-match-statistics']//div[contains(@class, 'section')][.//div[contains(@class, 'sectionHeader') and (contains(text(), 'Возврат') or contains(text(), 'Очки'))]]")
            
            self.logger.info(f"Найдено {len(sections)} секций статистики")
            
            for section in sections:
                # Определяем тип секции (Возврат или Очки)
                try:
                    section_header = section.find_element(By.CSS_SELECTOR, ".sectionHeader").text
                    self.logger.info(f"Обрабатываем секцию: {section_header}")
                except:
                    section_header = "Неизвестная секция"
                
                stats_rows = section.find_elements(By.CSS_SELECTOR, ".wcl-row_OFViZ[data-testid='wcl-statistics']")
                self.logger.debug(f"Найдено {len(stats_rows)} строк в секции {section_header}")
                
                for row in stats_rows:
                    try:
                        # Извлекаем категорию
                        category = row.find_element(By.CSS_SELECTOR, ".wcl-category_7qsgP strong").text
                        
                        # Извлекаем значения для домашнего и гостевого игроков
                        home_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-homeValue_-iJBW")
                        away_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-awayValue_rQvxs")
                        
                        # Извлекаем основное значение (в strong)
                        home_main_value = home_value_element.find_element(By.CSS_SELECTOR, "strong").text
                        away_main_value = away_value_element.find_element(By.CSS_SELECTOR, "strong").text
                        
                        # Проверяем наличие дополнительных значений (в span)
                        home_detail_value = ""
                        away_detail_value = ""
                        
                        try:
                            spans_home = home_value_element.find_elements(By.CSS_SELECTOR, "span")
                            if spans_home and len(spans_home) > 0:
                                for span in spans_home:
                                    if span.text and "(" in span.text:
                                        home_detail_value = span.text.strip("()")
                        except:
                            pass
                            
                        try:
                            spans_away = away_value_element.find_elements(By.CSS_SELECTOR, "span")
                            if spans_away and len(spans_away) > 0:
                                for span in spans_away:
                                    if span.text and "(" in span.text:
                                        away_detail_value = span.text.strip("()")
                        except:
                            pass
                        
                        # Формируем структуру с данными
                        if home_detail_value or away_detail_value:
                            category_with_section = f"{section_header} - {category}"
                            game_stats[category_with_section] = {
                                "home": {"value": home_main_value, "details": home_detail_value},
                                "away": {"value": away_main_value, "details": away_detail_value}
                            }
                        else:
                            category_with_section = f"{section_header} - {category}"
                            game_stats[category_with_section] = {
                                "home": home_main_value,
                                "away": away_main_value
                            }
                        
                        self.logger.debug(f"Обработана статистика: {category_with_section}")
                    except Exception as e:
                        self.logger.error(f"Ошибка при парсинге строки статистики: {str(e)}")
                        continue
            
            end_time = time.time()
            self.logger.info(f"Парсинг статистики возврата и очков завершен за {end_time - start_time:.2f} секунд")
            return game_stats
            
        except Exception as e:
            end_time = time.time()
            self.logger.error(f"Ошибка при парсинге статистики возврата и очков: {str(e)}")
            self.logger.error(f"Время выполнения с ошибкой: {end_time - start_time:.2f} секунд")
            return {}

    def parse_games_stats(self):
        """Парсинг статистики геймов"""
        try:
            self.logger.info("Начинаем парсинг статистики по геймам")
            start_time = time.time()
            games_stats = {}
            
            # Находим секцию геймов - используем указанный пользователем селектор
            try:
                games_section = WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, 
                    "//div[@data-analytics-context='tab-match-statistics']//div[contains(@class, 'section')][.//div[contains(@class, 'sectionHeader') and contains(text(), 'Геймы')]]"))
                )
                self.logger.info("Секция геймов найдена")
            except Exception as e:
                self.logger.error(f"Не удалось найти секцию геймов: {str(e)}")
                return {}
            
            # Парсим все строки статистики
            stats_rows = games_section.find_elements(By.CSS_SELECTOR, ".wcl-row_OFViZ[data-testid='wcl-statistics']")
            self.logger.info(f"Найдено {len(stats_rows)} строк статистики геймов")
            
            for row in stats_rows:
                try:
                    # Извлекаем категорию
                    category = row.find_element(By.CSS_SELECTOR, ".wcl-category_7qsgP strong").text
                    self.logger.debug(f"Обрабатываем категорию: {category}")
                    
                    # Извлекаем значения для домашнего и гостевого игроков
                    home_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-homeValue_-iJBW")
                    away_value_element = row.find_element(By.CSS_SELECTOR, ".wcl-awayValue_rQvxs")
                    
                    # Извлекаем основное значение (в strong)
                    home_main_value = home_value_element.find_element(By.CSS_SELECTOR, "strong").text
                    away_main_value = away_value_element.find_element(By.CSS_SELECTOR, "strong").text
                    
                    # Проверяем наличие дополнительных значений (в span)
                    home_detail_value = ""
                    away_detail_value = ""
                    
                    try:
                        spans_home = home_value_element.find_elements(By.CSS_SELECTOR, "span")
                        if spans_home and len(spans_home) > 0:
                            for span in spans_home:
                                if span.text and "(" in span.text:
                                    home_detail_value = span.text.strip("()")
                    except:
                        pass
                        
                    try:
                        spans_away = away_value_element.find_elements(By.CSS_SELECTOR, "span")
                        if spans_away and len(spans_away) > 0:
                            for span in spans_away:
                                if span.text and "(" in span.text:
                                    away_detail_value = span.text.strip("()")
                    except:
                        pass
                    
                    # Формируем структуру с данными
                    if home_detail_value or away_detail_value:
                        games_stats[category] = {
                            "home": {"percent": home_main_value, "numbers": home_detail_value},
                            "away": {"percent": away_main_value, "numbers": away_detail_value}
                        }
                    else:
                        games_stats[category] = {
                            "home": home_main_value,
                            "away": away_main_value
                        }
                    
                    self.logger.debug(f"Статистика для {category} успешно собрана")
                    
                except Exception as e:
                    self.logger.error(f"Ошибка при парсинге строки статистики геймов: {str(e)}")
                    continue
            
            end_time = time.time()
            self.logger.info(f"Парсинг статистики по геймам завершен за {end_time - start_time:.2f} секунд")
            return games_stats
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге статистики по геймам: {str(e)}")
            return {}

    def parse_odds(self):
        """Парсинг коэффициентов"""
        self.logger.info("Начало парсинга коэффициентов...")
        
        try:
            odds_info = {}
            
            # Ищем секцию коэффициентов в указанном селекторе
            try:
                odds_section = WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics'] .oddsWrapper"))
                )
                self.logger.info("Секция коэффициентов найдена")
            except Exception as e:
                self.logger.warning(f"Не удалось найти секцию коэффициентов в статистике: {str(e)}")
                
                # Пробуем найти секцию коэффициентов в любом месте страницы
                try:
                    odds_section = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".oddsWrapper, .odds"))
                    )
                    self.logger.info("Секция коэффициентов найдена в альтернативном месте")
                except Exception as e2:
                    self.logger.error(f"Не удалось найти секцию коэффициентов: {str(e2)}")
                    return {}
            
            # Ищем конкретные элементы коэффициентов - используем структуру, указанную пользователем
            try:
                odds_cells = odds_section.find_elements(By.CSS_SELECTOR, ".odds .cellWrapper")
                self.logger.info(f"Найдено {len(odds_cells)} ячеек коэффициентов")
                
                for cell in odds_cells:
                    try:
                        # Получаем тип коэффициента (1 или 2)
                        odds_type = cell.find_element(By.CSS_SELECTOR, ".oddsType").text
                        
                        # Получаем значение коэффициента
                        odds_value = cell.find_element(By.CSS_SELECTOR, ".oddsValueInner").text
                        
                        # Получаем направление изменения коэффициента (up/down)
                        odds_direction = ""
                        try:
                            odds_value_wrapper = cell.find_element(By.CSS_SELECTOR, ".oddsValue")
                            class_attr = odds_value_wrapper.get_attribute("class")
                            if "up" in class_attr:
                                odds_direction = "up"
                            elif "down" in class_attr:
                                odds_direction = "down"
                        except:
                            pass
                        
                        # Получаем исходное значение из title
                        odds_original = ""
                        try:
                            title_attr = cell.get_attribute("title")
                            if title_attr and " » " in title_attr:
                                odds_original = title_attr.split(" » ")[0]
                        except:
                            pass
                        
                        # Сохраняем данные
                        if odds_type == "1":
                            odds_info["home_odds"] = odds_value
                            if odds_direction:
                                odds_info["home_odds_direction"] = odds_direction
                            if odds_original:
                                odds_info["home_odds_original"] = odds_original
                        elif odds_type == "2":
                            odds_info["away_odds"] = odds_value
                            if odds_direction:
                                odds_info["away_odds_direction"] = odds_direction
                            if odds_original:
                                odds_info["away_odds_original"] = odds_original
                        
                        self.logger.debug(f"Обработан коэффициент типа {odds_type}: {odds_value}")
                    except Exception as e_cell:
                        self.logger.error(f"Ошибка при обработке ячейки коэффициента: {str(e_cell)}")
                        continue
                
                if not odds_info:
                    # Альтернативный метод поиска коэффициентов
                    self.logger.warning("Используем альтернативный метод поиска коэффициентов")
                    odds_elements = odds_section.find_elements(By.CSS_SELECTOR, ".cell")
                    
                    for element in odds_elements:
                        try:
                            odds_type_element = element.find_element(By.CSS_SELECTOR, ".oddsType")
                            odds_type = odds_type_element.text
                            
                            odds_value_element = element.find_element(By.CSS_SELECTOR, ".oddsValueInner")
                            odds_value = odds_value_element.text
                            
                            if odds_type == "1":
                                odds_info["home_odds"] = odds_value
                            elif odds_type == "2":
                                odds_info["away_odds"] = odds_value
                                
                            self.logger.debug(f"Альтернативно обработан коэффициент типа {odds_type}: {odds_value}")
                        except:
                            continue
            except Exception as e_parse:
                self.logger.error(f"Ошибка при парсинге коэффициентов: {str(e_parse)}")
            
            self.logger.info(f"Коэффициенты получены: {odds_info}")
            return odds_info
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге коэффициентов: {str(e)}")
            return {}

    def close_cookies_popup(self, driver):
        """Закрывает окно cookies, если оно есть (ищет кнопку 'Я принимаю')"""
        try:
            buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Я принимаю') or contains(text(), 'Принять') or contains(text(), 'Согласен') or contains(text(), 'Accept') or contains(text(), 'Agree')]")
            if buttons:
                buttons[0].click()
                time.sleep(1)
                self.logger.info("Окно cookies закрыто")
        except Exception as e:
            self.logger.debug(f"Не удалось закрыть окно cookies: {str(e)}")

    def parse_h2h_stats(self, match_url):
        """Парсинг H2H статистики для матча (использует основной драйвер)"""
        try:
            if "#/h2h" not in match_url:
                h2h_url = match_url.split("#")[0] + "#/h2h/overall"
            else:
                h2h_url = match_url

            self.logger.info(f"[H2H] Используем основной драйвер. Загрузка H2H страницы: {h2h_url}")
            self.driver.get(h2h_url)
            self.close_cookies_popup(self.driver)
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".h2h__section"))
                )
            except Exception as e:
                self.logger.warning(f"[H2H] Не удалось дождаться секции: {str(e)}")
                return {}
            try:
                WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".h2h__row"))
                )
            except:
                self.logger.warning("[H2H] Строки матчей не найдены, возможно, данных нет")
            # time.sleep(1.5)  # убираем лишний sleep

            h2h_data = {"home_last_matches": [], "away_last_matches": [], "mutual_matches": []}

            sections = self.driver.find_elements(By.CSS_SELECTOR, ".h2h__section.section")
            for section in sections:
                try:
                    header = section.find_element(By.CSS_SELECTOR, "[data-testid='wcl-headerSection-text']").text.strip().lower()
                    rows = section.find_elements(By.CSS_SELECTOR, ".h2h__row")
                    for row in rows:
                        try:
                            date = row.find_element(By.CSS_SELECTOR, ".h2h__date").text.strip()
                        except:
                            date = ""
                        try:
                            event = row.find_element(By.CSS_SELECTOR, ".h2h__event").text.strip()
                        except:
                            event = ""
                        try:
                            home = row.find_element(By.CSS_SELECTOR, ".h2h__homeParticipant .h2h__participantInner").text.strip()
                        except:
                            home = ""
                        try:
                            away = row.find_element(By.CSS_SELECTOR, ".h2h__awayParticipant .h2h__participantInner").text.strip()
                        except:
                            away = ""
                        try:
                            result = row.find_element(By.CSS_SELECTOR, ".h2h__result").text.strip().replace('\n', ':')
                        except:
                            result = ""
                        try:
                            outcome = row.find_element(By.CSS_SELECTOR, ".wcl-badgeform_yYFgV").text.strip()
                        except:
                            outcome = ""
                        match_str = f"{date} | {event} | {home} - {away} | {result} | {outcome}"
                        if "последние игры" in header:
                            if "тиафо" in header or "алькарас" in header:
                                h2h_data["home_last_matches"].append(match_str)
                            elif "муньяр" in header or "куинн" in header:
                                h2h_data["away_last_matches"].append(match_str)
                        elif "очные встречи" in header:
                            h2h_data["mutual_matches"].append(match_str)
                except Exception as e:
                    self.logger.warning(f"[H2H] Ошибка при обработке секции: {str(e)}")
            return h2h_data
        except Exception as e:
            self.logger.error(f"[H2H] Ошибка при парсинге H2H: {str(e)}")
            return {}

    def get_last_surface_match_stats(self, live_matches, output_json=None):
        """Для каждого live-матча ищет последний матч с нужным покрытием в H2H, кликает по покрытию, парсит статистику и сохраняет всё в отдельный JSON. Использует основной драйвер."""
        results = []
        for match in live_matches:
            try:
                match_url = match.get('url')
                if not match_url:
                    continue
                # Получаем ссылку на H2H
                if "#/h2h" not in match_url:
                    h2h_url = match_url.split("#")[0] + "#/h2h/overall"
                else:
                    h2h_url = match_url
                self.driver.get(h2h_url)
                self.close_cookies_popup(self.driver)
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".h2h__section"))
                    )
                except Exception as e:
                    self.logger.warning(f"[SURFACE] Не удалось дождаться секции: {str(e)}")
                    results.append({
                        "live_match_url": match_url,
                        "surface_match_url": None,
                        "surface_type": None,
                        "surface_match_stats": None
                    })
                    continue
                # time.sleep(1.5)  # убираем лишний sleep
                found = False
                rows = self.driver.find_elements(By.CSS_SELECTOR, ".h2h__row")
                for row in rows:
                    try:
                        event_span = row.find_element(By.CSS_SELECTOR, ".h2h__event")
                        classes = event_span.get_attribute("class")
                        if any(surf in classes for surf in ["hard surface", "clay surface", "grass surface"]):
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", event_span)
                            event_span.click()
                            try:
                                WebDriverWait(self.driver, 3).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics']"))
                                )
                            except Exception as e:
                                self.logger.warning(f"[SURFACE] Не удалось дождаться статистики после клика: {str(e)}")
                                continue
                            self.close_cookies_popup(self.driver)
                            stats = self.parse_match_details(self.driver.current_url)
                            results.append({
                                "live_match_url": match_url,
                                "surface_match_url": self.driver.current_url,
                                "surface_type": classes,
                                "surface_match_stats": stats
                            })
                            found = True
                            break
                    except Exception as e:
                        continue
                if not found:
                    results.append({
                        "live_match_url": match_url,
                        "surface_match_url": None,
                        "surface_type": None,
                        "surface_match_stats": None
                    })
            except Exception as e:
                self.logger.error(f"Ошибка в get_last_surface_match_stats: {str(e)}")
        # Сохраняем в отдельный JSON
        if not output_json:
            output_json = f"last_surface_matches_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Статистика surface-матчей сохранена в файл: {output_json}")
        return output_json

    def parse_match_details(self, match_url):
        """Парсинг детальной информации о матче"""
        total_start_time = time.time()
        self.logger.info(f"Начало парсинга матча: {match_url}")
        
        try:
            # Проверяем URL
            if not "flashscorekz.com/match/" in match_url:
                self.logger.error(f"Некорректный URL матча: {match_url}")
                return None
            
            # Корректируем URL для точного соответствия нужной вкладке
            if "/#/match-summary/match-statistics" not in match_url:
                base_match_url = match_url.split("#/")[0]
                match_url = f"{base_match_url}#/match-summary/match-statistics/0"
            
            self.logger.info(f"Загрузка страницы статистики матча: {match_url}")
            try:
                self.driver.get(match_url)
            except Exception as e:
                self.logger.error(f"Ошибка загрузки страницы {match_url}: {e}")
                return None
            
            # Проверяем и выбираем подвкладку "Матч" (если такая существует)
            try:
                # Ждем загрузки контента вкладки
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics'] .section"))
                )
                
                # Проверяем наличие подвкладок
                match_tabs = self.driver.find_elements(By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics'] a[title='Матч']")
                if match_tabs:
                    if "active" not in match_tabs[0].get_attribute("class"):
                        match_tabs[0].click()
                        self.logger.info("Выполнен клик по подвкладке 'Матч'")
                        time.sleep(2)
                    else:
                        self.logger.info("Подвкладка 'Матч' уже активна")
                else:
                    self.logger.info("Подвкладка 'Матч' не найдена, возможно, не требуется")
            except Exception as e_subtab:
                self.logger.warning(f"Ошибка при проверке/выборе подвкладки 'Матч': {str(e_subtab)}")
            
            # Получаем информацию об игроках - пробуем разные селекторы
            home_player = "Неизвестный игрок"
            away_player = "Неизвестный игрок"
            players_start = time.time()
            
            # Определяем формат страницы и извлекаем имена игроков
            try:
                player_selectors = [
                    ".duelParticipant__home .participant__participantName", 
                    ".event__participant--home", 
                    ".event__participant.event__participant--home",
                    "[class*='participant'][class*='home']",
                    ".home [class*='name']",
                    ".participant__participantName"
                ]
                
                for selector in player_selectors:
                    try:
                        home_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if home_elements and home_elements[0].text.strip():
                            home_player = home_elements[0].text.strip()
                            self.logger.info(f"Найден домашний игрок по селектору {selector}: {home_player}")
                            break
                    except:
                        continue
                
                away_selectors = [
                    ".duelParticipant__away .participant__participantName", 
                    ".event__participant--away", 
                    ".event__participant.event__participant--away",
                    "[class*='participant'][class*='away']",
                    ".away [class*='name']"
                ]
                
                for selector in away_selectors:
                    try:
                        away_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if away_elements and away_elements[0].text.strip():
                            away_player = away_elements[0].text.strip()
                            self.logger.info(f"Найден гостевой игрок по селектору {selector}: {away_player}")
                            break
                    except:
                        continue
                
                self.logger.info(f"Информация об игроках получена за {time.time() - players_start:.2f} секунд")
                
                # Если оба игрока "Неизвестный игрок", пробуем дополнительные методы
                if home_player == "Неизвестный игрок" and away_player == "Неизвестный игрок":
                    # Пробуем найти любые имена игроков на странице
                    try:
                        all_names = self.driver.find_elements(By.XPATH, 
                            "//*[contains(@class, 'participant') or contains(@class, 'name') or contains(@class, 'player')]")
                        player_names = [name.text.strip() for name in all_names if name.text.strip() and len(name.text.strip()) > 3]
                        
                        if len(player_names) >= 2:
                            home_player = player_names[0]
                            away_player = player_names[1]
                            self.logger.info(f"Извлечены имена игроков альтернативным методом: {home_player} vs {away_player}")
                    except Exception as e:
                        self.logger.error(f"Ошибка при альтернативном поиске имен игроков: {str(e)}")
            except Exception as e:
                self.logger.error(f"Ошибка при получении имен игроков: {str(e)}")
            
            # Получаем счет - пробуем разные селекторы и методы
            score_start = time.time()
            score_info = {
                "sets": "0-0",
                "current_set": "Неизвестно",
                "current_game": ""
            }
            
            try:
                # Пробуем разные селекторы для счета
                score_selectors = [
                    ".detailScore__wrapper",
                    ".event__scores",
                    ".event__score",
                    "[class*='score']",
                    ".score"
                ]
                
                for selector in score_selectors:
                    try:
                        score_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if score_elements:
                            score_text = score_elements[0].text.strip()
                            if score_text and any(c.isdigit() for c in score_text):
                                score_info["sets"] = score_text
                                self.logger.info(f"Найден счет по селектору {selector}: {score_text}")
                                break
                    except:
                        continue
                
                # Пробуем найти текущий сет
                set_selectors = [
                    ".fixedHeaderDuel__detailStatus",
                    ".event__status",
                    "[class*='status']",
                    ".status",
                    "div:contains('1-й сет'), div:contains('2-й сет'), div:contains('3-й сет')"
                ]
                
                for selector in set_selectors:
                    try:
                        set_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if set_elements:
                            set_text = set_elements[0].text.strip()
                            if set_text and ("сет" in set_text.lower() or "тай" in set_text.lower()):
                                score_info["current_set"] = set_text
                                self.logger.info(f"Найден текущий сет по селектору {selector}: {set_text}")
                                break
                    except:
                        continue
                
                # Если текущий сет не найден, ищем по XPath
                if score_info["current_set"] == "Неизвестно":
                    try:
                        set_elements = self.driver.find_elements(By.XPATH, "//div[contains(text(), '1-й сет') or contains(text(), '2-й сет') or contains(text(), '3-й сет') or contains(text(), 'тай-брейк')]")
                        if set_elements:
                            score_info["current_set"] = set_elements[0].text.strip()
                            self.logger.info(f"Найден текущий сет по XPath: {score_info['current_set']}")
                    except:
                        pass
                
                # Пробуем найти текущий гейм
                try:
                    game_elements = self.driver.find_elements(By.CSS_SELECTOR, ".detailScore__detailScoreServe, [class*='game'], [class*='currentScore']")
                    for element in game_elements:
                        game_text = element.text.strip()
                        if game_text and any(c.isdigit() for c in game_text):
                            score_info["current_game"] = game_text
                            self.logger.info(f"Найден счет текущего гейма: {game_text}")
                            break
                except:
                    pass
                
                self.logger.info(f"Информация о счете получена за {time.time() - score_start:.2f} секунд")
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге счета: {str(e)}")
            
            # Вместо блока с получением коэффициентов используем наш новый метод
            odds_info = self.parse_odds()
            
            # Формируем базовую информацию о матче
            match_info = {
                "home_player": home_player,
                "away_player": away_player,
                "score": score_info,
                "odds": odds_info,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "url": match_url,
                "has_statistics": False
            }
            
            # Отключаем скриншоты для ускорения
            # try:
            #     screenshot_path = f"match_stats_page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            #     self.driver.save_screenshot(screenshot_path)
            #     self.logger.info(f"Сохранен скриншот страницы статистики: {screenshot_path}")
            # except Exception as e:
            #     self.logger.error(f"Не удалось сделать скриншот: {str(e)}")
            
            # Парсим статистику с учетом указанных пользователем селекторов
            serve_stats = self.parse_serve_stats()
            game_stats = self.parse_game_stats()
            games_stats = self.parse_games_stats()

            # Отключаем парсинг H2H для ускорения
            # h2h_stats = self.parse_h2h_stats(match_url)
            # match_info["h2h_stats"] = h2h_stats
            
            if serve_stats or game_stats or games_stats:
                match_info["serve_stats"] = serve_stats
                match_info["game_stats"] = game_stats
                match_info["games_stats"] = games_stats
                match_info["has_statistics"] = True
                self.logger.info("Статистика успешно получена")
            else:
                match_info["statistics_message"] = "Статистика не найдена или пуста"
                self.logger.warning("Статистика не найдена или пуста")
            
            total_time = time.time() - total_start_time
            self.logger.info(f"Парсинг матча успешно завершен за {total_time:.2f} секунд")
            return match_info
            
        except Exception as e:
            total_time = time.time() - total_start_time
            self.logger.error(f"Ошибка при парсинге деталей матча: {str(e)}")
            self.logger.error(f"Общее время выполнения с ошибкой: {total_time:.2f} секунд")
            return None

    def get_live_matches(self):
        """Получение информации о всех live матчах"""
        try:
            match_links = self.get_match_links()
            matches_info = []
            # Общее количество матчей для прогресса
            total_matches = len(match_links)
            self.logger.info(f"Обрабатываем {total_matches} Live матчей")
            for idx, link in enumerate(match_links, 1):
                self.logger.info(f"Обработка матча {idx}/{total_matches}: {link}")
                match_info = self.parse_match_details(link)
                if match_info:
                    matches_info.append(match_info)
                    self.logger.info(f"Матч {idx}/{total_matches} успешно обработан")
                else:
                    self.logger.warning(f"Не удалось получить информацию о матче {idx}/{total_matches}")
                # Убираем задержку между матчами для максимальной скорости
                # self.logger.info(f"Пауза 2 секунд...")
                # time.sleep(2)
            self.logger.info(f"Успешно получена информация о {len(matches_info)} матчах из {total_matches}")
            return matches_info
        except Exception as e:
            self.logger.error(f"Ошибка при получении информации о матчах: {str(e)}")
            return []

    def save_to_json(self, events, filename='tennis_events.json'):
        """Сохранение событий в JSON файл"""
        try:
            # Проверяем наличие статистики геймов в каждом матче
            for event in events:
                if 'game_stats' not in event:
                    self.logger.warning(f"Отсутствует статистика геймов для матча {event.get('home_player', 'Unknown')} vs {event.get('away_player', 'Unknown')}")
                else:
                    self.logger.info(f"Статистика геймов для матча {event.get('home_player', 'Unknown')} vs {event.get('away_player', 'Unknown')}: {event['game_stats']}")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(events, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Данные успешно сохранены в файл {filename}")
            
            # Проверяем сохраненные данные
            with open(filename, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
                for event in saved_data:
                    if 'game_stats' in event:
                        self.logger.info(f"Проверка сохраненных данных: {event['home_player']} vs {event['away_player']}")
                        self.logger.info(f"Статистика геймов: {event['game_stats']}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении в файл: {str(e)}")

    def close(self):
        """Закрытие драйвера"""
        try:
            self.driver.quit()
            self.logger.info("WebDriver успешно закрыт")
        except Exception as e:
            self.logger.error(f"Ошибка при закрытии WebDriver: {str(e)}")

    def parse_specific_match(self, match_url):
        """Парсинг конкретного матча по URL"""
        try:
            # Проверяем, что URL содержит правильный формат
            if not match_url.startswith("https://www.flashscorekz.com/match/tennis/"):
                self.logger.error("Некорректный URL матча")
                return None
            
            # Добавляем /#/match-summary/match-statistics к URL если его нет
            if "/#/match-summary/match-statistics" not in match_url:
                match_url = f"{match_url}/#/match-summary/match-statistics"
            
            self.driver.get(match_url)
            
            # Ждем загрузки страницы статистики
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-analytics-context='tab-match-statistics']"))
                )
            except Exception as e:
                self.logger.error(f"Ошибка при ожидании загрузки страницы статистики: {str(e)}")
                return None
            
            # Получаем статистику
            match_info = {
                "serve_stats": self.parse_serve_stats(),
                "game_stats": self.parse_game_stats()
            }
            
            self.logger.info(f"Успешно получена статистика для матча: {match_url}")
            return match_info
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге матча: {str(e)}")
            return None

    def save_match_details(self, match_stats, match_url, filename=None):
        """Сохранение детальной информации о матче в JSON файл"""
        try:
            if filename is None:
                # Создаем имя файла на основе текущей даты и времени
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"match_stats_{timestamp}.json"
            
            # Добавляем URL матча в статистику
            match_stats['match_url'] = match_url
            match_stats['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Сохраняем в JSON файл
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(match_stats, f, ensure_ascii=False, indent=4)
            
            self.logger.info(f"Статистика матча сохранена в файл: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении статистики в файл: {str(e)}")
            return None

    def filter_and_send_live_matches(self, live_matches, telegram_token=None, telegram_chat_id=None):
        """Фильтрует live-матчи по условиям и отправляет лучшие в Telegram."""
        filtered = []
        for match in live_matches:
            stats = match.get('serve_stats', {})
            odds = match.get('odds', {})
            games_stats = match.get('games_stats', {})
            # Проверка коэффициентов
            try:
                home_odds = float(odds.get('home_odds', 0))
                away_odds = float(odds.get('away_odds', 0))
            except:
                continue
            odds_ok = (home_odds > 2.5 or away_odds > 2.5 or home_odds < 1.6 or away_odds < 1.6)
            # Первая подача
            first_serve = stats.get('1-я подача', {})
            try:
                home_first_serve = float(first_serve.get('home', '0').replace('%', '').replace(',', '.'))
                away_first_serve = float(first_serve.get('away', '0').replace('%', '').replace(',', '.'))
            except:
                home_first_serve = away_first_serve = 0
            serve_ok = (home_first_serve > 60 or away_first_serve > 60)
            # Выиграно очков на 1-й подаче
            first_serve_points = stats.get('Выиграно очков на 1-й подаче', {})
            try:
                home_first_serve_points = float(first_serve_points.get('home', '0').replace('%', '').replace(',', '.'))
                away_first_serve_points = float(first_serve_points.get('away', '0').replace('%', '').replace(',', '.'))
            except:
                home_first_serve_points = away_first_serve_points = 0
            points_ok = (home_first_serve_points > 60 or away_first_serve_points > 60)
            # Сыграно геймов (суммируем все значения в games_stats)
            total_games = 0
            for k, v in games_stats.items():
                try:
                    if isinstance(v, dict):
                        total_games += int(v.get('home', 0)) + int(v.get('away', 0))
                    else:
                        total_games += int(v)
                except:
                    continue
            games_ok = (1 <= total_games <= 5)
            # Считаем количество совпавших условий
            ok_count = sum([odds_ok, serve_ok, points_ok, games_ok])
            if ok_count >= 2:
                match['ok_count'] = ok_count
                filtered.append(match)
        # Формируем сообщение
        if filtered:
            filtered.sort(key=lambda m: m['ok_count'], reverse=True)
            msg = '<b>Лучшие live-матчи по фильтру:</b>\n'
            for match in filtered:
                msg += f"\n<b>{match.get('home_player', '?')} vs {match.get('away_player', '?')}</b>"
                msg += f"\nСчёт: {match.get('score', {}).get('sets', '?')} | Сет: {match.get('score', {}).get('current_set', '?')}"
                msg += f"\nКоэф.: {match.get('odds', {}).get('home_odds', '?')} / {match.get('odds', {}).get('away_odds', '?')}"
                msg += f"\n1-я подача: {match.get('serve_stats', {}).get('1-я подача', {})}"
                msg += f"\nВыигр. очков на 1-й: {match.get('serve_stats', {}).get('Выиграно очков на 1-й подаче', {})}"
                msg += f"\nГеймы: {match.get('games_stats', {})}"
                msg += f"\n<a href='{match.get('url', '')}'>Ссылка на матч</a>"
                msg += f"\nСовпавших условий: {match['ok_count']}\n"
        else:
            msg = 'Нет подходящих live-матчей по фильтру.'
        # Отправляем в Telegram
        if telegram_token and telegram_chat_id:
            url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
            data = {'chat_id': telegram_chat_id, 'text': msg, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
            try:
                requests.post(url, data=data)
            except Exception as e:
                self.logger.error(f"Ошибка отправки в Telegram: {str(e)}")
        return filtered, msg

    def format_games_stats(self, games_stats, favorite_side):
        """Форматирует статистику геймов для красивого отображения"""
        if not games_stats:
            return 'нет данных'
        lines = []
        for k, v in games_stats.items():
            if isinstance(v, dict):
                fav = v.get(favorite_side, {})
                opp = v.get('away' if favorite_side == 'home' else 'home', {}) if favorite_side == 'home' else v.get('home', {})
                fav_percent = fav.get('percent', fav if isinstance(fav, str) else '')
                fav_numbers = fav.get('numbers', '')
                opp_percent = opp.get('percent', opp if isinstance(opp, str) else '')
                opp_numbers = opp.get('numbers', '')
                line = f"{k}: {fav_numbers} ({fav_percent}) | соперник: {opp_numbers} ({opp_percent})"
                lines.append(line)
            else:
                lines.append(f"{k}: {v}")
        return '\n'.join(lines)

    async def filter_and_send_favorites(self, live_matches, telegram_bot, chat_id):
        filtered = []
        used_ids = set()
        # 1. Основной фильтр (строгий)
        for match in live_matches:
            odds = match.get('odds', {})
            serve_stats = match.get('serve_stats', {})
            games_stats = match.get('games_stats', {})
            url = match.get('url', '')
            source_url = match.get('source_url', '')
            home = match.get('home_player', '?')
            away = match.get('away_player', '?')
            container_home = match.get('container_home', '')
            container_away = match.get('container_away', '')
            url_note = ''
            if url and source_url and url != source_url:
                self.logger.warning(f"[URL MISMATCH] {home} vs {away}: source_url={source_url}, parsed_url={url}")
                url_note += f"\n⚠️ <b>Внимание: ссылка после парсинга отличается от исходной!</b>\nИсходная: {source_url}\nПарсинг: {url}"
            player_note = ''
            parsed_home = home.strip().lower()
            parsed_away = away.strip().lower()
            cont_home = container_home.strip().lower()
            cont_away = container_away.strip().lower()
            if (parsed_home and cont_home and parsed_home != cont_home) or (parsed_away and cont_away and parsed_away != cont_away):
                self.logger.warning(f"[PLAYER MISMATCH] Контейнер: {container_home} vs {container_away} | Парсинг: {home} vs {away}")
                player_note = f"\n⚠️ <b>Внимание: имена игроков после парсинга отличаются от исходных!</b>\nКонтейнер: {container_home} vs {container_away}\nПарсинг: {home} vs {away}"
            # HOME
            try:
                home_odds = float(odds.get('home_odds', 0))
                home_first_serve = None
                for key in ['% первой подачи', '1-я подача']:
                    if key in serve_stats:
                        val = serve_stats[key].get('home')
                        if isinstance(val, dict):
                            val = val.get('value') or val.get('percent') or list(val.values())[0]
                        home_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                        break
                home_first_serve_points = None
                for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                    if key in serve_stats:
                        val = serve_stats[key].get('home')
                        if isinstance(val, dict):
                            val = val.get('value') or val.get('percent') or list(val.values())[0]
                        home_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                        break
                if home_odds > 2.2 and home_first_serve and home_first_serve > 60 and home_first_serve_points and home_first_serve_points > 60:
                    filtered.append({
                        'side': 'home',
                        'player': home,
                        'opponent': away,
                        'odds': home_odds,
                        'first_serve': home_first_serve,
                        'first_serve_points': home_first_serve_points,
                        'games_stats': games_stats,
                        'url': url,
                        'url_note': url_note,
                        'player_note': player_note,
                        '_match_id': f"{url}_home",
                        'serve_stats': serve_stats
                    })
                    used_ids.add(f"{url}_home")
            except Exception as e:
                pass
            # AWAY
            try:
                away_odds = float(odds.get('away_odds', 0))
                away_first_serve = None
                for key in ['% первой подачи', '1-я подача']:
                    if key in serve_stats:
                        val = serve_stats[key].get('away')
                        if isinstance(val, dict):
                            val = val.get('value') or val.get('percent') or list(val.values())[0]
                        away_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                        break
                away_first_serve_points = None
                for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                    if key in serve_stats:
                        val = serve_stats[key].get('away')
                        if isinstance(val, dict):
                            val = val.get('value') or val.get('percent') or list(val.values())[0]
                        away_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                        break
                if away_odds > 2.2 and away_first_serve and away_first_serve > 60 and away_first_serve_points and away_first_serve_points > 60:
                    filtered.append({
                        'side': 'away',
                        'player': away,
                        'opponent': home,
                        'odds': away_odds,
                        'first_serve': away_first_serve,
                        'first_serve_points': away_first_serve_points,
                        'games_stats': games_stats,
                        'url': url,
                        'url_note': url_note,
                        'player_note': player_note,
                        '_match_id': f"{url}_away",
                        'serve_stats': serve_stats
                    })
                    used_ids.add(f"{url}_away")
            except Exception as e:
                pass
        # 2. Добор по сумме трёх компонентов в зависимости от количества фаворитов
        need_count = 3 - len(filtered)
        if need_count > 0:
            candidates = []
            for match in live_matches:
                odds = match.get('odds', {})
                serve_stats = match.get('serve_stats', {})
                url = match.get('url', '')
                home = match.get('home_player', '?')
                away = match.get('away_player', '?')
                games_stats = match.get('games_stats', {})
                # HOME
                try:
                    home_odds = float(odds.get('home_odds', 0))
                    home_first_serve = None
                    for key in ['% первой подачи', '1-я подача']:
                        if key in serve_stats:
                            val = serve_stats[key].get('home')
                            if isinstance(val, dict):
                                val = val.get('value') or val.get('percent') or list(val.values())[0]
                            home_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                            break
                    home_first_serve_points = None
                    for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                        if key in serve_stats:
                            val = serve_stats[key].get('home')
                            if isinstance(val, dict):
                                val = val.get('value') or val.get('percent') or list(val.values())[0]
                            home_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                            break
                    if home_odds >= 2.3 and home_first_serve is not None and home_first_serve_points is not None:
                        score_sum = home_odds + home_first_serve + home_first_serve_points
                        match_id = f"{url}_home"
                        if match_id not in used_ids:
                            candidates.append({
                                'side': 'home',
                                'player': home,
                                'opponent': away,
                                'odds': home_odds,
                                'first_serve': home_first_serve,
                                'first_serve_points': home_first_serve_points,
                                'games_stats': games_stats,
                                'url': url,
                                'score_sum': score_sum,
                                '_match_id': match_id,
                                'serve_stats': serve_stats
                            })
                except Exception as e:
                    pass
                # AWAY
                try:
                    away_odds = float(odds.get('away_odds', 0))
                    away_first_serve = None
                    for key in ['% первой подачи', '1-я подача']:
                        if key in serve_stats:
                            val = serve_stats[key].get('away')
                            if isinstance(val, dict):
                                val = val.get('value') or val.get('percent') or list(val.values())[0]
                            away_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                            break
                    away_first_serve_points = None
                    for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                        if key in serve_stats:
                            val = serve_stats[key].get('away')
                            if isinstance(val, dict):
                                val = val.get('value') or val.get('percent') or list(val.values())[0]
                            away_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                            break
                    if away_odds >= 2.3 and away_first_serve is not None and away_first_serve_points is not None:
                        score_sum = away_odds + away_first_serve + away_first_serve_points
                        match_id = f"{url}_away"
                        if match_id not in used_ids:
                            candidates.append({
                                'side': 'away',
                                'player': away,
                                'opponent': home,
                                'odds': away_odds,
                                'first_serve': away_first_serve,
                                'first_serve_points': away_first_serve_points,
                                'games_stats': games_stats,
                                'url': url,
                                'score_sum': score_sum,
                                '_match_id': match_id,
                                'serve_stats': serve_stats
                            })
                except Exception as e:
                    pass
            # Сортируем кандидатов по сумме и добираем нужное количество
            candidates.sort(key=lambda x: x['score_sum'], reverse=True)
            for cand in candidates:
                if len(filtered) >= 3:
                    break
                filtered.append(cand)
                used_ids.add(cand['_match_id'])
        # Оставляем только топ-3 по коэффициенту (или сумме)
        top_matches = filtered[:3]
        for fav in top_matches:
            games_msg = self.format_games_stats(fav['games_stats'], fav['side'])
            # Получаем % выигранных очков на второй подаче
            serve_stats = None
            if 'side' in fav and fav['side'] == 'home':
                serve_stats = fav.get('serve_stats') or None
                if serve_stats is None:
                    serve_stats = next((m.get('serve_stats') for m in live_matches if m.get('url') == fav['url']), None)
                second_serve_key = None
                for key in ['Очки выигр. на в.п.', 'Очки выигр. на 2-й подаче']:
                    if serve_stats and key in serve_stats:
                        second_serve_key = key
                        break
                if serve_stats and second_serve_key:
                    val = serve_stats[second_serve_key].get('home')
                    if isinstance(val, dict):
                        second_serve = val.get('value') or val.get('percent') or list(val.values())[0]
                    else:
                        second_serve = val
                else:
                    second_serve = 'нет данных'
            elif 'side' in fav and fav['side'] == 'away':
                serve_stats = fav.get('serve_stats') or None
                if serve_stats is None:
                    serve_stats = next((m.get('serve_stats') for m in live_matches if m.get('url') == fav['url']), None)
                second_serve_key = None
                for key in ['Очки выигр. на в.п.', 'Очки выигр. на 2-й подаче']:
                    if serve_stats and key in serve_stats:
                        second_serve_key = key
                        break
                if serve_stats and second_serve_key:
                    val = serve_stats[second_serve_key].get('away')
                    if isinstance(val, dict):
                        second_serve = val.get('value') or val.get('percent') or list(val.values())[0]
                    else:
                        second_serve = val
                else:
                    second_serve = 'нет данных'
            else:
                second_serve = 'нет данных'
            msg = (
                f"<b>Матч:</b> {fav['player']} vs {fav['opponent']}\n"
                f"<b>Фаворит:</b> {fav['player']}\n"
                f"<b>Коэффициент:</b> {fav['odds']}\n"
                f"<b>% первой подачи:</b> {fav['first_serve']}%\n"
                f"<b>% выигр. очков на 1-й подаче:</b> {fav['first_serve_points']}%\n"
                f"<b>% выигр. очков на второй подаче:</b> {second_serve}\n"
                f"<b>Геймы фаворита:</b>\n{games_msg}"
                f"<a href='{fav['url']}'>Ссылка на матч</a>"
                f"{fav.get('url_note','')}{fav.get('player_note','')}"
            )
            await telegram_bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_web_page_preview=True)

    async def send_summary_to_telegram(self, live_matches, telegram_bot, chat_id):
        summary = '<b>Сводка по live-матчам:</b>\n'
        for match in live_matches:
            odds = match.get('odds', {})
            serve_stats = match.get('serve_stats', {})
            url = match.get('url', '')
            home = match.get('home_player', '?')
            away = match.get('away_player', '?')
            try:
                home_odds = float(odds.get('home_odds', 0))
            except:
                home_odds = None
            try:
                away_odds = float(odds.get('away_odds', 0))
            except:
                away_odds = None
            home_first_serve = None
            for key in ['% первой подачи', '1-я подача']:
                if key in serve_stats:
                    val = serve_stats[key].get('home')
                    if isinstance(val, dict):
                        val = val.get('value') or val.get('percent') or list(val.values())[0]
                    home_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                    break
            away_first_serve = None
            for key in ['% первой подачи', '1-я подача']:
                if key in serve_stats:
                    val = serve_stats[key].get('away')
                    if isinstance(val, dict):
                        val = val.get('value') or val.get('percent') or list(val.values())[0]
                    away_first_serve = float(str(val).replace('%', '').replace(',', '.'))
                    break
            home_first_serve_points = None
            for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                if key in serve_stats:
                    val = serve_stats[key].get('home')
                    if isinstance(val, dict):
                        val = val.get('value') or val.get('percent') or list(val.values())[0]
                    home_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                    break
            away_first_serve_points = None
            for key in ['Очки выигр. на п.п.', 'Очки выигр. на 1-й подаче']:
                if key in serve_stats:
                    val = serve_stats[key].get('away')
                    if isinstance(val, dict):
                        val = val.get('value') or val.get('percent') or list(val.values())[0]
                    away_first_serve_points = float(str(val).replace('%', '').replace(',', '.'))
                    break
            # Фильтрация: только если все значения есть
            if (home_first_serve is None or away_first_serve is None or home_first_serve_points is None or away_first_serve_points is None):
                continue
            home_ok = sum([
                home_odds is not None and home_odds > 2,
                home_first_serve is not None and home_first_serve > 60,
                home_first_serve_points is not None and home_first_serve_points > 60
            ]) >= 2
            away_ok = sum([
                away_odds is not None and away_odds > 2,
                away_first_serve is not None and away_first_serve > 60,
                away_first_serve_points is not None and away_first_serve_points > 60
            ]) >= 2
            match_line = f"\n<a href='{url}'>{home} — {away}</a>"
            match_line += f"\nКоэф: {home_odds if home_odds is not None else '?'} / {away_odds if away_odds is not None else '?'}"
            match_line += f"\n% первой подачи: {home_first_serve if home_first_serve is not None else '?'}% / {away_first_serve if away_first_serve is not None else '?'}%"
            match_line += f"\nОчки выигр. на п.п.: {home_first_serve_points if home_first_serve_points is not None else '?'}% / {away_first_serve_points if away_first_serve_points is not None else '?'}%"
            if home_ok:
                match_line = f"\n<b>⭐️ {home} — {away}</b>" + match_line[len(f"\n{home} — {away}"):] + "\n<b>Фаворит: {}</b>".format(home)
            elif away_ok:
                match_line = f"\n<b>⭐️ {home} — {away}</b>" + match_line[len(f"\n{home} — {away}"):] + "\n<b>Фаворит: {}</b>".format(away)
            summary += match_line + '\n'
        await telegram_bot.send_message(chat_id=chat_id, text=summary, parse_mode='HTML', disable_web_page_preview=True)

TELEGRAM_TOKEN = '8076439766:AAEFdBhJqDfZWHTwK0H5A8-vVJgVERE9PdY'

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("Старт парсинга", callback_data='start_parsing')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('Нажми кнопку для запуска парсинга live-матчей:', reply_markup=reply_markup)

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == 'start_parsing':
        await query.edit_message_text(text="Парсинг запущен! Ожидайте прогресс...")
        chat_id = query.message.chat_id
        asyncio.create_task(run_parsing_and_send(chat_id, context))

def parse_one_match(link):
    parser = TennisParser()
    try:
        result = parser.parse_match_details(link)
    except Exception as e:
        result = None
    finally:
        parser.close()
    return result

async def run_parsing_and_send(chat_id, context):
    parser = TennisParser()
    try:
        await context.bot.send_message(chat_id=chat_id, text="Начинаем парсинг Live матчей...")
        live_matches = []
        total = 0
        try:
            match_links = parser.get_match_links()  # Теперь urls — список ссылок
            total = len(match_links)
            await context.bot.send_message(chat_id=chat_id, text=f"Найдено live-матчей: {total}")
            for idx, match in enumerate(match_links, 1):
                urls = match['urls'] if 'urls' in match else [match['url']]
                container_home = match.get('home_player', '')
                container_away = match.get('away_player', '')
                found = False
                for link in urls:
                    match_info = parser.parse_match_details(link)
                    if match_info:
                        parsed_home = match_info.get('home_player', '').strip().lower()
                        parsed_away = match_info.get('away_player', '').strip().lower()
                        cont_home = container_home.strip().lower()
                        cont_away = container_away.strip().lower()
                        if parsed_home == cont_home and parsed_away == cont_away:
                            match_info['home_player'] = container_home
                            match_info['away_player'] = container_away
                            match_info['source_url'] = link
                            match_info['container_home'] = container_home
                            match_info['container_away'] = container_away
                            live_matches.append(match_info)
                            found = True
                            break
                if not found and urls:
                    match_info = parser.parse_match_details(urls[0])
                    if match_info:
                        match_info['home_player'] = container_home
                        match_info['away_player'] = container_away
                        match_info['source_url'] = urls[0]
                        match_info['container_home'] = container_home
                        match_info['container_away'] = container_away
                        match_info['fallback_link'] = True
                        live_matches.append(match_info)
                await context.bot.send_message(chat_id=chat_id, text=f"Обработано {idx} из {total} матчей...")
                await asyncio.sleep(0.5)
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Ошибка при парсинге: {str(e)}")
        # Сохраняем live-матчи
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = f"live_matches_{timestamp}.json"
        parser.save_to_json(live_matches, filename=json_file)
        await context.bot.send_message(chat_id=chat_id, text=f"Информация о Live матчах сохранена в файл: {json_file}")
        # Отправляем сам файл в Telegram
        try:
            with open(json_file, "rb") as f:
                await context.bot.send_document(chat_id=chat_id, document=f, filename=json_file)
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Не удалось отправить файл: {e}")
        # Новый: отправка фаворитов по твоим условиям
        await parser.filter_and_send_favorites(live_matches, context.bot, chat_id)
        # Отправка сводного отчёта
        await parser.send_summary_to_telegram(live_matches, context.bot, chat_id)
    finally:
        parser.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))
    print("Telegram-бот запущен. Ожидает команду /start...")
    app.run_polling() 