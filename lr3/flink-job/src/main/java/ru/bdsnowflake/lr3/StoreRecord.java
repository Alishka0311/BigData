package ru.bdsnowflake.lr3;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public record StoreRecord(
        String storeKey,
        String name,
        String location,
        String city,
        String state,
        String country,
        String phone,
        String email
) {
    public static StoreRecord fromEvent(RawSaleEvent event) {
        return new StoreRecord(
                PetShopStreamingJob.stableKey(
                        "store",
                        event.store_name,
                        event.store_location,
                        event.store_city,
                        event.store_country
                ),
                event.store_name,
                event.store_location,
                event.store_city,
                event.store_state,
                event.store_country,
                event.store_phone,
                event.store_email
        );
    }

    public static void bind(PreparedStatement statement, StoreRecord record) throws SQLException {
        statement.setString(1, record.storeKey);
        statement.setString(2, record.name);
        statement.setString(3, record.location);
        statement.setString(4, record.city);
        statement.setString(5, record.state);
        statement.setString(6, record.country);
        statement.setString(7, record.phone);
        statement.setString(8, record.email);
    }
}
