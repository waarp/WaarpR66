/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.context;

import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;

/**
 * @author Frederic Bregier
 *
 */
public interface R66BusinessInterface {
    
    public void checkAtStartup(R66Session session) throws OpenR66RunnerErrorException;
    public void checkAfterPreCommand(R66Session session) throws OpenR66RunnerErrorException;
    public void checkAfterTransfer(R66Session session) throws OpenR66RunnerErrorException;
    public void checkAfterPost(R66Session session) throws OpenR66RunnerErrorException;
    public void checkAtError(R66Session session) throws OpenR66RunnerErrorException;
    public void checkAtChangeFilename(R66Session session) throws OpenR66RunnerErrorException;
    public void releaseResources();
    public String getInfo();
    public void setInfo(String info);
    
}
