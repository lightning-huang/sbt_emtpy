package basic;

public class OfCourseUtil {
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean isNullOrWhiteSpace(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static boolean isEquals(String a, String b) {
        return isEquals(a, b, false);
    }

    public static boolean isEquals(String a, String b, Boolean ignoreCase) {
        boolean doubleNull = (a == null) && (b == null);
        boolean singleNull = (a == null && b != null) || (a != null && b == null);

        if (doubleNull || singleNull) {
            return doubleNull ? true : false;
        }

        if (ignoreCase) {
            a = a.toLowerCase();
            b = b.toLowerCase();
        }

        return a.equals(b);
    }
}
