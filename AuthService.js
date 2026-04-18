/**
 * AuthService.gs
 * Role-based authentication and session management for G2N portals
 * Validates staff codes against LU_Staff in G2N_Lookups,
 * manages user sessions via PropertiesService, and provides
 * role-based access control (Staff vs Admin).
 * v3.0 - Role-based access via LU_Staff lookup
 * v3.1 - Fixed isStaff() to verify role instead of just checking authentication (#13).
 * v3.2 - Adopted trimHeaders() in getStaffMember/getAllStaff to prevent whitespace
 *         header-match failures. Adopted isRowActive() for Active column checks.
 * v3.3 - getStaffMember() and getAllStaff() now delegate to
 *         LookupService.getStaffRecords() instead of reading LU_Staff
 *         directly. Eliminates duplicate sheet-parsing logic and ensures
 *         consistent behavior if staff sheet structure changes (#5).
 */

/**
 * Authenticates a user by staff code against LU_Staff
 * Stores session data in UserProperties; logs LOGIN to AuditLog
 * @param {string} staffCode - The staff code entered by the user
 * @returns {Object} { success: boolean, role: string, name: string }
 */
function authenticateUser(code) {
    if (!code || code.trim() === '') {
        return { success: false, error: 'Please enter your staff code' };
    }

    const staffData = getStaffMember(code.trim().toUpperCase());

    if (!staffData) {
        return { success: false, error: 'Staff code not found' };
    }

    if (!staffData.active) {
        return { success: false, error: 'This account is inactive. Please contact an administrator.' };
    }

    // Store session
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('G2N_USER_CODE', staffData.code);
    userProperties.setProperty('G2N_USER_NAME', staffData.name);
    userProperties.setProperty('G2N_USER_ROLE', staffData.role);
    userProperties.setProperty('G2N_LOGIN_TIME', new Date().toISOString());

    // Log the login
    logAudit('LOGIN', null, 'User logged in: ' + staffData.code + ' (' + staffData.name + ')');

    return {
        success: true,
        user: {
            code: staffData.code,
            name: staffData.name,
            role: staffData.role,
            isAdmin: staffData.role === 'Administrator'
        }
    };
}

/**
 * Returns the current authenticated user's session data
 * @returns {Object|null} { code, name, role } or null if not authenticated
 */
function getCurrentUser() {
    const userProperties = PropertiesService.getUserProperties();
    const code = userProperties.getProperty('G2N_USER_CODE');

    if (!code) {
        return null;
    }

    return {
        code: code,
        name: userProperties.getProperty('G2N_USER_NAME'),
        role: userProperties.getProperty('G2N_USER_ROLE'),
        isAdmin: userProperties.getProperty('G2N_USER_ROLE') === 'Administrator',
        loginTime: userProperties.getProperty('G2N_LOGIN_TIME')
    };
}

/**
 * Clears the current user session; logs LOGOUT to AuditLog
 * @returns {Object} { success: boolean }
 */
function logoutUser() {
    const user = getCurrentUser();

    if (user) {
        logAudit('LOGOUT', null, 'User logged out: ' + user.code);
    }

    const userProperties = PropertiesService.getUserProperties();
    userProperties.deleteProperty('G2N_USER_CODE');
    userProperties.deleteProperty('G2N_USER_NAME');
    userProperties.deleteProperty('G2N_USER_ROLE');
    userProperties.deleteProperty('G2N_LOGIN_TIME');

    return { success: true };
}

/**
 * Checks if the current user has an active session
 * @returns {boolean}
 */
function isAuthenticated() {
    return getCurrentUser() !== null;
}

/**
 * Checks if the current user has Staff or Admin role
 * v3.1 - Now verifies role instead of just checking authentication
 * @returns {boolean}
 */
function isStaff() {
    const user = getCurrentUser();
    return user !== null && (user.role === 'Staff' || user.role === 'Administrator');
}

/**
 * Get staff member data from LU_Staff via LookupService
 * v3.3 - Delegates to getStaffRecords() (#5)
 * @param {string} code - Staff code
 * @returns {Object|null} Staff data or null if not found
 */
function getStaffMember(code) {
    var result = getStaffRecords(true); // Include inactive to check active status
    if (!result.success || !result.records) return null;
    var upperCode = code.toUpperCase();
    for (var i = 0; i < result.records.length; i++) {
        var rec = result.records[i];
        if (rec.Code.toUpperCase() === upperCode) {
            return {
                code: rec.Code,
                name: rec.Name,
                role: rec.Role || 'Staff',
                active: rec.Active
            };
        }
    }
    return null;
}

/**
 * Get all active staff members via LookupService
 * v3.3 - Delegates to getStaffRecords() (#5)
 * @returns {Array} Array of staff objects
 */
function getAllStaff() {
    var result = getStaffRecords(false); // Active only
    if (!result.success || !result.records) return [];
    return result.records.map(function (r) {
        return { code: r.Code, name: r.Name, role: r.Role || 'Staff' };
    });
}

/**
 * Throws an error if the user is not authenticated
 * Use as a guard at the start of protected functions
 * @throws {Error} If not authenticated
 */
function requireAuth() {
    if (!isAuthenticated()) {
        throw new Error('Authentication required. Please log in.');
    }
    return getCurrentUser();
}
