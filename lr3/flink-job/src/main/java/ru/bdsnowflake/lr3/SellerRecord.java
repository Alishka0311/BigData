package ru.bdsnowflake.lr3;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public record SellerRecord(
        String sellerKey,
        Long sellerSourceId,
        String firstName,
        String lastName,
        String email,
        String country,
        String postalCode
) {
    public static SellerRecord fromEvent(RawSaleEvent event) {
        return new SellerRecord(
                PetShopStreamingJob.stableKey(
                        "seller",
                        event.seller_first_name,
                        event.seller_last_name,
                        event.seller_email,
                        event.seller_country
                ),
                PetShopStreamingJob.longValue(event.sale_seller_id),
                event.seller_first_name,
                event.seller_last_name,
                event.seller_email,
                event.seller_country,
                event.seller_postal_code
        );
    }

    public static void bind(PreparedStatement statement, SellerRecord record) throws SQLException {
        statement.setString(1, record.sellerKey);
        if (record.sellerSourceId == null) {
            statement.setNull(2, java.sql.Types.BIGINT);
        } else {
            statement.setLong(2, record.sellerSourceId);
        }
        statement.setString(3, record.firstName);
        statement.setString(4, record.lastName);
        statement.setString(5, record.email);
        statement.setString(6, record.country);
        statement.setString(7, record.postalCode);
    }
}
