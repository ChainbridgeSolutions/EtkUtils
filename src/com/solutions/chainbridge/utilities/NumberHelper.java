/**
 *
 * Helper methods for (safe) numeric conversion and compare.
 *
 * psmiley 03/02/2017
 **/

package com.solutions.chainbridge.utilities;


public class NumberHelper {

    // Safe compare, generic but slower...
    public static boolean isEqual(Number numLeft, Number numRight) {
        boolean retval = Boolean.FALSE;

        if ((numLeft != null) && (numRight != null)) {
            // Compare...
            if (numLeft.getClass().equals(numRight.getClass()))
                retval = numLeft.equals(numRight);
            else if (numLeft.getClass().equals(Long.class) || numLeft.getClass().equals(Integer.class))
                retval = (numLeft.longValue() == numRight.longValue());
            else
                retval = numLeft.toString().equals(numRight);
        } else if ((numLeft == null) && (numRight == null)) {
            retval = Boolean.TRUE;
        }

        return retval;
    }

    // Safe compare...
    public static boolean isEqualLong(Number numLeft, Number numRight) {
        boolean retval = Boolean.FALSE;

        if ((numLeft != null) && (numRight != null)) {
            // Compare as long...
            retval = (numLeft.longValue() == numRight.longValue());
        } else if ((numLeft == null) && (numRight == null)) {
            retval = Boolean.TRUE;
        }

        return retval;
    }
}
