/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.context;

/**
 * Empty Business factory
 * @author Frederic Bregier
 *
 */
public class R66DefaultBusinessFactory implements R66BusinessFactoryInterface {

    /* (non-Javadoc)
     * @see openr66.context.R66BusinessFactoryInterface#getBusinessInterface(openr66.context.R66Session)
     */
    @Override
    public R66BusinessInterface getBusinessInterface(R66Session session) {
        return null;
    }

    /* (non-Javadoc)
     * @see openr66.context.R66BusinessFactoryInterface#releaseResources()
     */
    @Override
    public void releaseResources() {
    }

}
