"""James Godkin
HenryDAO is all the MYSQL calls to the HenryTable"""

import mysql.connector

class henryDAO():
    def __init__(self):
        self.mydb = mysql.connector.connect(
            user='root',
            passwd='sql!sC00l',
            database='comp3421',
            host='127.0.0.1')

        self.mycur = self.mydb.cursor()

    def close(self):
        self.mydb.commit()
        self.mydb.close()

    def getAuthorName(self):
        # Perform the query
        sql = "SELECT distinct(concat(henry_author.AUTHOR_FIRST, ' ' ,henry_author.AUTHOR_LAST)) as name, henry_author.AUTHOR_NUM \
               FROM henry_author \
               JOIN henry_wrote on henry_wrote.AUTHOR_NUM = henry_author.AUTHOR_NUM \
               WHERE henry_wrote.AUTHOR_NUM is not NULL \
               ORDER BY name"
        self.mycur.execute(sql)

        # results into a list
        listOfAuthors = []
        for row in self.mycur:
            listOfAuthors.append([row[0], int(row[1])])
        return listOfAuthors

    def getAuthorBooks(self, author):
        sql = "select TITLE, BOOK_CODE \
                from henry_book \
                where BOOK_CODE in ( \
                    select BOOK_CODE \
                    from henry_wrote \
                    where AUTHOR_NUM = %s)"
        self.mycur.execute(sql, (author,))

        listOfBooks = []
        for row in self.mycur:
            listOfBooks.append([row[0], str(row[1])])
        return listOfBooks

    def getStoreInfo(self, book):
        sql = "select henry_branch.BRANCH_NAME, henry_inventory.ON_HAND \
                from henry_branch \
                join henry_inventory on henry_inventory.BRANCH_NUM = henry_branch.BRANCH_NUM \
                where BOOK_CODE = %s"
        self.mycur.execute(sql, (book,))

        listOfStoreInfo = []
        for row in self.mycur:
            listOfStoreInfo.append(row)
        return listOfStoreInfo

    def getPrice(self, book):
        sql = "select price \
                from henry_book \
                where book_code = %s"
        self.mycur.execute(sql, (book,))

        listOfPrice = []
        for row in self.mycur:
            listOfPrice = float(row[0])
        return listOfPrice

    def getCategory(self):
        sql = "SELECT distinct(TYPE) \
                       FROM henry_book \
                       ORDER BY TYPE"
        self.mycur.execute(sql)

        # results into a list
        listOfCategory = []
        for row in self.mycur:
            listOfCategory.append(row[0])
        return listOfCategory

    def getCategoryBooks(self, category):
        sql = "select TITLE, BOOK_CODE \
                from henry_book \
                where TYPE = %s"
        self.mycur.execute(sql, (category,))

        listOfBooks = []
        for row in self.mycur:
            listOfBooks.append([row[0], str(row[1])])
        return listOfBooks

    def getPublisherName(self):
        sql = "select distinct(henry_publisher.PUBLISHER_NAME), henry_publisher.PUBLISHER_CODE \
               from henry_publisher \
               JOIN henry_book on henry_book.PUBLISHER_CODE = henry_publisher.PUBLISHER_CODE \
               WHERE henry_book.PUBLISHER_CODE is not NULL \
               ORDER BY henry_publisher.PUBLISHER_NAME"
        self.mycur.execute(sql)

        listOfPublisher = []
        for row in self.mycur:
            listOfPublisher.append([row[0], row[1]])
        return listOfPublisher

    def getPublisherBooks(self, publisher):
        sql = "select TITLE, BOOK_CODE \
                from henry_book \
                where PUBLISHER_CODE = %s"
        self.mycur.execute(sql, (publisher,))

        listOfBooks = []
        for row in self.mycur:
            listOfBooks.append([row[0], str(row[1])])
        return listOfBooks

# test = henryDAO()
# print(test.getPublisherBooks('BY'))
# test.close()