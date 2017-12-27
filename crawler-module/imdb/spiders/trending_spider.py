import scrapy
import json
import csv
import re
from datetime import date
from imdb.items import ImdbItem
import re

trending_page = 'http://www.the-numbers.com/movies/trending'
suggest_link = 'https://v2.sg.media-imdb.com/suggests/'
imdb_link = 'http://www.imdb.com/title/'
country_page = "http://www.imdb.com/country/"
year_page = "http://www.imdb.com/year/"
fileout = "MANUAL_test_2.csv"
crawl_image = True

class YelpSpider(scrapy.Spider):
    name = "trending"
    allowed_domains = ['the-numbers.com','imdb.com','v2.sg.media-imdb.com']
    # start_urls = [
    #     'https://www.yelp.com/search?cflt=restaurants&find_loc=501',
    # ]
    # handle_httpstatus_list = [503]

    def __init__(self):
        self.items = set()
        self.ids = set()
        header = ['Idx','Id','Title','Year','Genres','Directors','Writers','Actors','Countries','Release Date','Release Date 1','Release Date 2','Runtime','Rating','Rating Count','Popularity','MetaScore','PeopleMayLike','Keywords','Link','Description','poster','slate']
        with open(fileout, 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_ALL)
            spamwriter.writerow(header)

    def start_requests(self):
        # yield scrapy.Request(country_page)
        yield scrapy.Request(trending_page)
        
    # def parse(self, response):
    #     main = response.css('div#main')
    #     tables = main.css('table.splash')
    #     print(len(tables))
    #     for table in tables:
    #         countries = table.css('tr td a')
    #         for country in countries:
    #             name = country.css('::text').extract()
    #             if len(name) > 0:
    #                 name = name[0].strip()
    #             else:
    #                 name = ''
    #             link = country.css('::attr(href)').extract()
    #             if len(link) > 0:
    #                 link = 'http://www.imdb.com' + link[0].strip()
    #             else:
    #                 link = ''
    #             # print('name: %s - link: %s' %(name, link))
    #             yield response.follow(link, callback=self.parse1)
    #             break
    #         break
    #         
    def parse(self, response):
        main = response.css('div#main')
        trendingItems = main.css('div[style="border: 1px solid black; border-radius: 8px; padding: 6px; margin: 8px; box-shadow: 4px 4px 4px #888;"]')
        for item in trendingItems:
            idx = item.css('div#col1 span::text').extract()
            idx = idx[0].strip() if idx else ''
            nameitem = item.css('div#col2outer div#col2mid table tr td span[style="font-size:200%;"] a')
            name = nameitem.css('::text').extract()
            name = name[0].strip() if name else ''
            if len(name) and len(idx):
                rename = re.sub('[^A-Za-z0-9\s]+', '', name)
                rename  = rename.replace(' ', '_')
                print(rename)
                link = suggest_link + rename[0].lower() + '/' + rename + '.json'
                yield response.follow(link, callback=self.parseJson, meta={'name':name, 'idx': idx})
                # break
    def parseJson(self, response):
        # print(response.text)
        idx = response.meta['idx']
        if('imdb$' in response.text):
            if '(' in response.text:
                pos = response.text.index('(')
                text = response.text[pos+1:-1]
                if len(text):
                    print(text)
                    searchJson = json.loads(text)
                    if 'd' in searchJson:
                        id = searchJson['d'][0]['id']
                        if id:
                            link = imdb_link + id
                            print('-----------following link: ' + link)
                            yield response.follow(link, callback=self.parse2, meta={'Id':id, 'Link': link, 'idx': idx})
                    else: 
                        print('!!!!!!!!!!!!!!!!!!!!! Not found movie in IMDB, retrying...!!! link: %s' %text)
                        name=response.meta['name'].split(':')
                        if len(name) > 1:
                            rename = re.sub('[^A-Za-z0-9\s]+', '', name[1])
                            rename  = rename.replace(' ', '_')
                            print(rename)
                            link = suggest_link + rename[0].lower() + '/' + rename + '.json'
                            yield response.follow(link, callback=self.parseJson, meta={'name':'', 'idx': idx})
        else:
            print('!!!!!!!!!!!!!!!!!!!!! Not found movie in IMDB!!! link: %s' %response.request.url)

    def parse1(self, response):
        main = response.css('div#main')
        listMovies = main.css('div.lister.list.detail.sub-list div.lister-list div.lister-item.mode-advanced')
        # i = 0
        for movie in listMovies:
            # i += 1
            # if i > 5:
            #     break
            movieid = movie.css('div.lister-top-right div.ribbonize::attr(data-tconst)').extract()
            movieid = movieid[0].strip() if len(movieid) else ''

            name = movie.css('div.lister-item-content h3.lister-item-header a::text').extract()
            name = name[0].strip() if len(name) else ''

            link = movie.css('div.lister-item-content h3.lister-item-header a::attr(href)').extract()
            link = link[0].strip() if len(link) else ''
            
            year = movie.css('div.lister-item-content h3.lister-item-header span.lister-item-year::text').extract()
            year = year[0].strip() if len(year) else ''
            if movieid not in self.ids:
                self.ids.add(movieid)
                yield response.follow(link, callback=self.parse2, meta={'Id':movieid, 'Link':'http://www.imdb.com' + link})
            else:
                print('Duplicate for movie id: %s' %movieid)
        navs = main.css('div.lister.list.detail.sub-list div.nav')
        if len(navs) > 0:
            nav = navs[0]
            nextPage = nav.css('div.desc a.lister-page-next.next-page::attr(href)').extract()
            if len(nextPage) > 0:
                print('Nexting page.........................')
                yield response.follow(nextPage[0], callback=self.parse1)
    def parse2(self, response):
        movie = []
        main = response.css('div#content-2-wide')
        maintop = main.css('div#main_top div.title-overview div#title-overview-widget')
        mainbottom = main.css('div#main_bottom')
        titlecast = main.css('div#titleCast')
        storyline = main.css('div#titleStoryLine')
        detail = main.css('div#titleDetails')

        titlebarwrapper =  maintop.css('div.vital div.title_block div.title_bar_wrapper')
        slatewrapper =  maintop.css('div.vital div.slate_wrapper')

        poster = slatewrapper.css('div.poster a img::attr(src)').extract()
        poster = (poster[0].split('_V1_')[0] + '_V1_.jpg') if len(poster) else ''
        if poster == '':
            poster = maintop.css('div.minPosterWithPlotSummaryHeight div.poster a img[itemprop="image"]::attr(src)').extract()
            poster = (poster[0].split('_V1_')[0] + '_V1_.jpg') if len(poster) else ''

        slate = slatewrapper.css('div.slate a.slate_button.prevent-ad-overlay.video-modal::attr(href)').extract()
        slate = 'http://www.imdb.com' + slate[0].strip() if len(slate) else ''
        
        title = titlebarwrapper.css('div.titleBar div.title_wrapper h1[itemprop="name"]::text').extract()
        title = title[0].strip() if len(title) else ''

        year = titlebarwrapper.css('div.titleBar div.title_wrapper h1[itemprop="name"] span#titleYear a::text').extract()
        year = year[0].strip() if len(year) else ''

        releasedate = titlebarwrapper.css('div.titleBar div.title_wrapper div.subtext a[title="See more release dates"]::text').extract()
        releasedate = releasedate[0].strip() if len(releasedate) else ''

        releasedate1 = titlebarwrapper.css('div.titleBar div.title_wrapper div.subtext a[title="See more release dates"] meta[itemprop="datePublished"]::attr(content)').extract()
        releasedate1 = releasedate1[0].strip() if len(releasedate1) else ''

        ratingbar = titlebarwrapper.css('div.ratings_wrapper div.imdbRating')
        rating = ratingbar.css('div.ratingValue strong span[itemprop="ratingValue"]::text').extract()
        rating = rating[0].strip() if len(rating) else ''

        ratingcount = ratingbar.css('a span[itemprop="ratingCount"]::text').extract()
        ratingcount = ratingcount[0].strip() if len(ratingcount) else ''

        plotsummary = maintop.css('div.plot_summary_wrapper div.plot_summary')
        description = plotsummary.css('div.summary_text::text').extract()
        description = description[0].strip() if len(description) else ''

        directorslist = plotsummary.css('div.credit_summary_item span[itemprop="director"] a span[itemprop="name"]::text').extract()
        directors = ','.join(directorslist)

        writerslist = plotsummary.css('div.credit_summary_item span[itemprop="creator"] a span[itemprop="name"]::text').extract()
        writers = ','.join(writerslist)

        titlereviewbar = maintop.css('div.plot_summary_wrapper div.titleReviewBar ')

        metascore = titlereviewbar.css('div.titleReviewBarItem a div.titleReviewBarSubItem span::text').extract()
        metascore = metascore[0].strip() if len(metascore) else ''

        popularity = titlereviewbar.css('div.titleReviewBarItem div.titleReviewBarSubItem div span.subText::text').extract()
        for pop in popularity:
            number = re.findall(r'\d+',pop)
            if len(number) > 0:
                popularity = number[0]
                break
        try:
            test = int(popularity)
            pass
        except Exception as e:
            popularity = ''
        
        peoplealsolikelist = mainbottom.css('div#titleRecs div#title_recs div.rec_const_picker div.rec_view div.rec_slide div.rec_page')
        peoplemaylike = peoplealsolikelist[0].css('div.rec_item::attr(data-tconst)').extract() if len(peoplealsolikelist) else ''
        peoplemaylike = ','.join(peoplemaylike)

        castslist = titlecast.css('table.cast_list tr td.itemprop[itemprop="actor"] a span.itemprop::text').extract()
        actors = ','.join(castslist)

        keywordslist = storyline.css('div.see-more.inline.canwrap[itemprop="keywords"] a span[itemprop="keywords"]::text').extract() 
        keywords = ','.join(keywordslist)

        genreslist = storyline.css('div.see-more.inline.canwrap[itemprop="genre"] a::text').extract()
        genreslist = ','.join(genreslist)
        
        itemslist = detail.css('div.txt-block')
        countrieslist = []
        countries = ''
        releasedate2 = ''
        runtime = ''
        for item in itemslist:
            itemname = item.css('h4.inline::text').extract()
            itemname = itemname[0].strip() if len(itemname) else ''
            if itemname == 'Country:':
                countrieslist = item.css('a[itemprop="url"]::text').extract()
                countries = ','.join(countrieslist)
            if itemname == 'Release Date:':
                texts = item.css('::text').extract()
                for text in texts:
                    if len(re.findall(r'\d+',text)) > 0:
                        releasedate2 = text.split('(')[0].strip()
                        break
            if itemname == 'Runtime:':
                runtime = item.css('time[itemprop="duration"]::text').extract()
                runtime = runtime[0].strip() if len(runtime) else ''
        if runtime == '':
            runtime = titlebarwrapper.css('div.titleBar div.title_wrapper div.subtext time[itemprop="duration"]::text').extract()
            runtime = runtime[0].strip() if len(runtime) else ''
        if year == '':
            texts = releasedate2.split(' ')
            year = texts[len(texts) - 1]

        imdbItem = ImdbItem()
        imdbItem['Idx']              = response.meta['idx']
        imdbItem['Id']              = response.meta['Id']
        imdbItem['Title']           = title
        imdbItem['Year']            = year
        imdbItem['Genres']          = genreslist
        imdbItem['Directors']       = directors
        imdbItem['Writers']         = writers
        imdbItem['Actors']          = actors
        imdbItem['Countries']       = countries
        imdbItem['ReleaseDate']     = releasedate
        imdbItem['ReleaseDate1']    = releasedate1
        imdbItem['ReleaseDate2']    = releasedate2
        imdbItem['Runtime']         = runtime
        imdbItem['Rating']          = rating
        imdbItem['RatingCount']     = ratingcount
        imdbItem['Popularity']      = popularity
        imdbItem['MetaScore']       = metascore
        imdbItem['PeopleMayLike']   = peoplemaylike
        imdbItem['Keywords']        = keywords
        imdbItem['Link']            = response.meta['Link']
        imdbItem['Description']     = description.replace('\"','')
        imdbItem['image_urls']      = [poster] if (len(poster.strip()) > 0 and crawl_image) else []
        imdbItem['file_urls']       = [slate] if len(slate.strip()) > 0 else []

        with open(fileout, 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_ALL)
            spamwriter.writerow([response.meta['idx'], response.meta['Id'], title, year, genreslist, directors, writers, actors, countries, 
                releasedate, releasedate1, releasedate2, runtime, rating, ratingcount, popularity, metascore, peoplemaylike, keywords, imdbItem['Link'], imdbItem['Description'], poster, slate])
        yield imdbItem
        