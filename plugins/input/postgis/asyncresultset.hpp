/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2011 Artem Pavlenko
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *****************************************************************************/

#ifndef POSTGIS_ASYNCRESULTSET_HPP
#define POSTGIS_ASYNCRESULTSET_HPP

#include <mapnik/debug.hpp>

#include "connection_manager.hpp"
#include "resultset.hpp"
#include <mapnik/datasource.hpp>

class AsyncResultSet : public IResultSet, private boost::noncopyable
{
public:
    AsyncResultSet(mapnik::processor_context_ptr const& ctx, boost::shared_ptr< Pool<Connection,ConnectionCreator> > const& pool, boost::shared_ptr<Connection> const &conn, const std::string& sql )
        : ctx_(ctx),
          pool_(pool),
          conn_(conn),
          sql_(sql),
          is_closed_(false),
          refCount_(new int(1))
    {
    }

    virtual bool use_connection() { return true; }


    virtual ~AsyncResultSet()
    {

        if (--(*refCount_)==0)
        {
            close();
            delete refCount_,refCount_=0;
        }
    }


    virtual void close()
    {
        if (!is_closed_)
        {
            rs_.reset();
            is_closed_ = true;
            if (conn_)
            {
                pool_->returnObject(conn_);
                conn_.reset();
            }
        }
    }

    virtual int getNumFields() const
    {
        return rs_->getNumFields();
    }

    virtual bool next()
    {
        bool next_res = false;
        if (!rs_)
        {
            if (conn_)
            {
                rs_ = conn_->getAsyncResult();
            }
            else
            {
                conn_ = pool_->borrowObject();
                if (conn_ && conn_->isOK())
                {
                    conn_->executeAsyncQuery(sql_, 1);
                    rs_ = conn_->getAsyncResult();
                }
                else
                {
                    throw mapnik::datasource_exception("Postgis Plugin: bad connection");
                }
            }
        }

        next_res = rs_->next();
        if (!next_res)
        {
            rs_.reset();
            rs_ = conn_->getAsyncResult(false);
            if (rs_ && rs_->next())
            {
                return true;
            }
            close();
        }
        return next_res;
    }

    virtual const char* getFieldName(int index) const
    {
        return rs_->getFieldName(index);
    }

    virtual int getFieldLength(int index) const
    {
        return rs_->getFieldLength(index);
    }

    virtual int getFieldLength(const char* name) const
    {
        return rs_->getFieldLength(name);
    }

    virtual int getTypeOID(int index) const
    {
        return rs_->getTypeOID(index);
    }

    virtual int getTypeOID(const char* name) const
    {
        return rs_->getTypeOID(name);
    }

    virtual bool isNull(int index) const
    {
        return rs_->isNull(index);
    }

    virtual const char* getValue(int index) const
    {
        return rs_->getValue(index);
    }

    virtual const char* getValue(const char* name) const
    {
        return rs_->getValue(name);
    }

private:
    mapnik::processor_context_ptr ctx_;
    boost::shared_ptr< Pool<Connection,ConnectionCreator> > pool_;
    boost::shared_ptr<Connection> conn_;
    std::string sql_;
    boost::shared_ptr<ResultSet> rs_;
    bool is_closed_;
    int *refCount_;


};

#endif // POSTGIS_ASYNCRESULTSET_HPP
