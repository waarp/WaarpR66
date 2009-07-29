/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.context.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;

/**
 * This class is used with external process in order to get the last echo
 * from the stdout of the process.
 * @author Frederic Bregier
 *
 */
class LastLineReader implements Runnable {
    private final BufferedReader reader;
    /**
     * This will be the result at the end
     */
    public String lastLine = null;

    public LastLineReader(PipedInputStream inputStream) {
        reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    lastLine = line;
                }
            }
        } catch (IOException e) {
            // Could be a "Write end dead", which means the end of the thread
            // writer is found
            // before the thread closes the write pipe
        }
        try {
            reader.close();
        } catch (IOException e) {
        }
    }

}
