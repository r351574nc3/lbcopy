/*
 * Copyright 2005-2007 The Kuali Foundation
 *
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kualigan.tools.ant.tasks;

/**
 * Ant type for adding regex attributes to searching sets.
 *
 * @author Leo Przybylski (przybyls@arizona.edu)
 */
public class Include {
    private String regex;

    
    /**
     * Gets the value of regex
     *
     * @return the value of regex
     */
    public final String getRegex() {
        return this.regex;
    }
     
    /**
     * Sets the value of regex
     *
     * @param argRegex Value to assign to this.regex
     */
    public final void setRegex(final String argRegex) {
        this.regex = argRegex;
    }
}
