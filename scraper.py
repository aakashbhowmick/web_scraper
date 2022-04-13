import requests as req
import bs4
import re
import pandas as pd
import datetime as dt
import os
import time


class SeenUnseenScraper:
    """ A book scraper for the website 'seenunseen.in'
    """
    
    base_url = 'https://seenunseen.in/episodes/'
    ep_url_pattern = r'https?://seenunseen.in/episodes/([0-9]+)/([0-9]+)/([0-9]+)/episode-([0-9]+)-.+'
    target_url_pattern = r'https?://[w.]*amazon.[a-z]+'
    
    
    def __init__(self, year, req_delay_seconds=None, log_filepath=None):
        """ 
            Args:
                year(int) : year for which data is to be fetched
                req_delay_second(int) : Delay (in seconds) to add between each request [to prevent IP from getting blocklisted]
                log_filepath(str) : Path to file for logging
            
            Returns:
                bs4.BeautifulSoup : On success, URL contents as aBeautifulSoup object
                NoneType : On failure
        """
        
        self.base_url_year = self.base_url + str(year) + '/'
        self.req_delay_second = req_delay_seconds
        self.log_filepath = log_filepath
        self.log_file = None
        
    
    def __get_page_soup(self, url):
        """ Given a URL return its corresponding bsoup object 
            Args:
                url(str) : URL to fetch
            
            Returns:
                bs4.BeautifulSoup : On success, URL contents as aBeautifulSoup object
                NoneType : On failure
        """
        try:
            result = req.get(url)
        except req.exceptions.RequestException as exp:
            # Request failed. Log this
            self.__write_log('Failed getting URL : {url} ; Error : {exp}')
            return None 
        else:
            return bs4.BeautifulSoup(result.content, "html.parser")

        
    def __get_episode_info(self, ep_url):
        """ Given an episode url return date and number
            Args:
                ep_url(str) : A URL for an episode
            Returns:
                tuple : On success, A 2-tuple containing (<episode number>, <episode date>)
                NoneType : If given url does not conform to the ep_url_pattern 
        """
        result = re.match(self.ep_url_pattern, ep_url)
        if not result: return None

        ep_date = dt.datetime(year  = int(result.group(1)),
                              month = int(result.group(2)),
                              day   = int(result.group(3)))
        ep_num = int(result.group(4))
        return ep_num, ep_date

    
    def __is_episode_url(self, url):
        """ Given a url check if it conforms to regex pattern 'ep_url_pattern'
            Args:
                url(str) : A URL 
            Returns:
                Bool : True if url matches regex pattern, False otherwise
        """
        result = re.match(self.ep_url_pattern, url)
        return True if result else False

    
    def __is_target_url(self, url):
        """ Given a url check if it conforms to regex pattern 'target_url_pattern'
            Args:
                url(str) : A URL 
            Returns:
                Bool : True if url matches regex pattern, False otherwise
        """
        result = re.match(self.target_url_pattern, url)
        return True if result else False
    
    
    def __write_log(self, msg):
        ''' Writes message to logfile with timestamp '''
        
        if self.log_file:
            timestamp_str = '{0:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now())
            self.log_file.write(f'[ {timestamp_str} ] : {msg}\n')
    
    
    def __get_books_as_dataframe(self):
        """ Scrapes seenunseen.com and returns dataframe for books found in each
            episode
            Args: None
            Returns:
                NoneType : If no book is found
                pd.DataFrame : A dataframe with the following schema :
                     'Episode'  : (int)
                     'Book'     : (str)
                     
                    |    Episode   |     Book    |
                    +----------------------------+
                    |     0        |   'Book0'   |
                    
        """

        # Fetch base page for year
        base_page = self.__get_page_soup(self.base_url_year)
        self.__write_log('Base page fetched')
        
        # Fetch all episode URLS on this page
        ep_urls = set([a['href'] for a in base_page.findAll('a') if self.__is_episode_url(a['href'])])
        
        # Fetch each episode URL
        df_list = []
        for ep_url in ep_urls:
            
            # Insert some delay, if needed
            if self.req_delay_second:
                time.sleep(self.req_delay_second)
            
            ep_page = self.__get_page_soup(ep_url)
            if not ep_page: continue  # Skip episode if page could not be fetched
            else : self.__write_log(f'Episode fetched : {ep_url}')
                
            # Get URL text for all Amazon URLS 
            amazon_books  = [a.contents[0] for a in ep_page.findAll('a') if self.__is_target_url(a['href'])]
            
            if len(amazon_books) == 0: continue
                
            ep_info = self.__get_episode_info(ep_url)
            ep_num  = ep_info[0]
            df_ep_books = pd.DataFrame({
                                'Episode' : pd.Series([ep_num]*len(amazon_books), dtype=int),
                                'Book'   : pd.Series(amazon_books, dtype=str)
                            })
            df_list.append(df_ep_books)
        
        if len(df_list)==0:
            return None  # No books found
        else:
            df_all_books = pd.concat(df_list, ignore_index=True)
            return df_all_books
        
        
    def get_books(self):
        """ Scrapes seenunseen.com and returns dataframe for books found in each
            episode
            Args: None
            Returns:
                NoneType : If no book is found
                pd.DataFrame : A dataframe with the following schema :
                     'Episode'  : (int)
                     'Book'     : (str)
                     
                    |    Episode   |     Book    |
                    +----------------------------+
                    |       0      |   'Book0'   |
                    
        """
        self.log_file = open(self.log_filepath, 'a') if self.log_filepath else None
        
        try:
            result = self.__get_books_as_dataframe()
        except Exception as e:
            raise e
        else:
            return result
        finally:
            if self.log_file:
                self.log_file.close()
                self.log_file = None


if __name__ == "__main__":
    proj_path = '/hdd/home_dir/aakash/exp/o9_proj/'
    scraper = SeenUnseenScraper(year=2021, log_filepath=os.path.join(proj_path, 'log.txt'))
    df_books = scraper.get_books()
    df_books.to_csv(os.path.join(proj_path, 'books.csv'), header=True, index=False)
