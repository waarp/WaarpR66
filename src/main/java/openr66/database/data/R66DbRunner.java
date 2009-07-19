/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.database.data;

import openr66.task.TaskRunner.TaskStatus;

/**
 * @author Frederic Bregier
 *
 */
public class R66DbRunner extends AbstractDbData {
    private int globalstep;

    private int step;

    private int rank;

    private TaskStatus status;

    // FIXME need a special ID
    private long specialId;

    private boolean isRetrieve;

    private String filename;

    private boolean isFileMoved = false;

    private String ruleId;

    private boolean isSaved = false;

    /**
     * @param specialId
     * @param globalstep
     * @param step
     * @param rank
     * @param status
     * @param isRetrieve
     * @param filename
     * @param isFileMoved
     */
    public R66DbRunner(long specialId, int globalstep, int step, int rank, TaskStatus status,
            boolean isRetrieve, String filename,
            boolean isFileMoved, String idRule) {
        this.globalstep = globalstep;
        this.step = step;
        this.rank = rank;
        this.status = status;
        this.specialId = specialId;
        this.isRetrieve = isRetrieve;
        this.filename = filename;
        this.isFileMoved = isFileMoved;
        this.ruleId = idRule;
        this.isSaved= false;
    }

    /**
     * @param specialId
     */
    public R66DbRunner(long specialId) {
        this.specialId = specialId;
        // FIXME load from DB
        this.isSaved= false;
    }
    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    protected void delete() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#getXML()
     */
    @Override
    protected String getXML() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#insert()
     */
    @Override
    protected void insert() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#select()
     */
    @Override
    protected void select() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#update()
     */
    @Override
    protected void update() {
        // TODO Auto-generated method stub

    }


}
