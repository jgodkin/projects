"""James Godkin
HenryInterfaceClasses creates and stores lists  from HenryDOA to be used in Henry"""

from HenryDAO import henryDAO

class author():
    def __init__(self, authors):
        self.authors = authors
        self.listOfAuthors = []
        self.listOfAuthorsID = []
        self.bookList = []
        self.bookCodeList = []
        self.storeInfoList = []
        self.price = 0
        self.listOfAuthors.append('-select-')
        self.listOfAuthorsID.append('-select-')

    def authorList(self):
        for i in range(len(self.authors)):
            self.listOfAuthors.append(self.authors[i][0])
            self.listOfAuthorsID.append(self.authors[i][1])
        return self.listOfAuthors

    def getBooks(self, index):
        self.bookList = []
        self.bookCodeList = []
        self.bookList.append('-select-')
        self.bookCodeList.append('-select-')
        books = henryDAO().getAuthorBooks(self.listOfAuthorsID[index])
        for i in range(len(books)):
            self.bookList.append(books[i][0])
            self.bookCodeList.append(books[i][1])
        return self.bookList

    def getInfo(self, book):
        self.storeInfoList = []
        self.storeInfoList.extend(henryDAO().getStoreInfo(self.bookCodeList[book]))
        return self.storeInfoList

    def getPrice(self, book):
        self.price = henryDAO().getPrice(self.bookCodeList[book])
        return self.price

class category():
    def __init__(self, category):
        self.category = category
        self.categoryList = ['-select-']
        self.bookList = []
        self.bookCodeList = []
        self.storeInfoList = []
        self.price = 0

    def getCategoryList(self):
        self.categoryList.extend(self.category)
        return self.categoryList

    def getBooks(self, index):
        self.bookList = []
        self.bookCodeList = []
        self.bookList.append('-select-')
        self.bookCodeList.append('-select-')
        books = henryDAO().getCategoryBooks(self.categoryList[index])
        for i in range(len(books)):
            self.bookList.append(books[i][0])
            self.bookCodeList.append(books[i][1])
        return self.bookList

    def getInfo(self, book):
        self.storeInfoList = []
        self.storeInfoList.extend(henryDAO().getStoreInfo(self.bookCodeList[book]))
        return self.storeInfoList

    def getPrice(self, book):
        self.price = henryDAO().getPrice(self.bookCodeList[book])
        return self.price

class publisher():
    def __init__(self, publisher):
        self.publisher = publisher
        self.publisherNameList = ['-select-']
        self.publisherIDList = ['-select-']
        self.bookList = []
        self.bookCodeList = []
        self.storeInfoList = []
        self.price = 0

    def getPublisherList(self):
        for i in range(len(self.publisher)):
            self.publisherNameList.append(self.publisher[i][0])
            self.publisherIDList.append(self.publisher[i][1])
        return self.publisherNameList

    def getBooks(self, index):
        self.bookList = []
        self.bookCodeList = []
        self.bookList.append('-select-')
        self.bookCodeList.append('-select-')
        books = henryDAO().getPublisherBooks(self.publisherIDList[index])
        for i in range(len(books)):
            self.bookList.append(books[i][0])
            self.bookCodeList.append(books[i][1])
        return self.bookList

    def getInfo(self, book):
        self.storeInfoList = []
        self.storeInfoList.extend(henryDAO().getStoreInfo(self.bookCodeList[book]))
        return self.storeInfoList

    def getPrice(self, book):
        self.price = henryDAO().getPrice(self.bookCodeList[book])
        return self.price

# test = author()
# print(test.authorList())