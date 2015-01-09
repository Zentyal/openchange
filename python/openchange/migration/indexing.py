#!/usr/bin/python
# -*- coding: utf-8 -*-

# Indexing DB schema and its migrations
# Copyright (C) Enrique J. Hernández Blasco <ejhernandez@zentyal.com> 2015
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Schema migration for Indexing "app" with SQL based backend
"""
from openchange.migration import migration, Migration


@migration('indexing', 1)
class InitialIndexingMigration(Migration):

    description = 'initial'

    @classmethod
    def apply(cls, cur):
        try:
            cur.execute('SELECT COUNT(*) FROM `mapistore_indexing`')
            return False
        except:
            # Table does not exist, then migrate
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS `mapistore_indexing` (
                         `id` INT NOT NULL AUTO_INCREMENT,
                         `username` VARCHAR(1024) NOT NULL,
                         `fmid` VARCHAR(36) NOT NULL,
                         `url` VARCHAR(1024) NOT NULL,
                         `soft_deleted` VARCHAR(36) NOT NULL,
                         PRIMARY KEY (`id`))
                       ENGINE = InnoDB""")

        cur.execute("""CREATE TABLE IF NOT EXISTS `mapistore_indexes` (
                         `id` INT NOT NULL AUTO_INCREMENT,
                         `username` VARCHAR(1024) NOT NULL,
                         `next_fmid` VARCHAR(36) NOT NULL,
                         PRIMARY KEY (`id`))
                       ENGINE = InnoDB""")

    @classmethod
    def unapply(cls, cur):
        for query in ("DROP TABLE mapistore_indexes",
                      "DROP TABLE mapistore_indexing"):
            cur.execute(query)
